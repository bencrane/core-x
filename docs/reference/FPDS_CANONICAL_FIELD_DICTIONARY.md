# FPDS Canonical — Field Dictionary & Rebuild Menu

_Authoritative field definitions from the USAspending Data Dictionary (457 elements, fetched 2026-07-02); joined to the 378-column bulk source `transaction_search_fpds` and the current 88-column canonical spine. Native USAspending/SAM.gov column names are preserved verbatim._

**Status:** 74 bulk cols on the spine today · 121 recommended to add this rebuild · 183 remaining (mostly internal/derived/`_raw`/statistical).

**Legend:** ✅ IN = on the spine today · ➕ PROPOSE = recommended add (this rebuild) · ⬜ out = in bulk, not carried · 🔒 internal = pg/derived/statistical, no USAspending definition


## ➕ Recommended additions (batch into the one rebuild)


### Socioeconomic & business-type flags (39)

| native column | type | definition | domain values |
|---|---|---|---|
| `alaskan_native_owned_corpo` | bool | https://www.sam.gov | F = False T = True |
| `alaskan_native_servicing_i` | bool | https://www.sam.gov | F = False T = True |
| `american_indian_owned_busi` | bool | List characteristic of the contractor such as whether the selected contractor is an American Indian Owned Business or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `black_american_owned_busin` | bool | List characteristic of the contractor such as whether the selected contractor is a Black American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `community_developed_corpor` | bool | https://www.sam.gov | F = False T = True |
| `community_development_corp` | bool | https://www.sam.gov | F = False T = True |
| `corporate_entity_not_tax_e` | bool | https://www.sam.gov | F = False T = True |
| `corporate_entity_tax_exemp` | bool | https://www.sam.gov | F = False T = True |
| `domestic_or_foreign_e_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Domestic or Foreign Entity Field. | A = U.S. OWNED BUSINESS B = OTHER U.S. ENTITY (E.G. GOVERNMENT) C = FOREIGN-OWNE |
| `domestic_or_foreign_entity` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Domestic or Foreign Entity Field. | A = U.S. OWNED BUSINESS B = OTHER U.S. ENTITY (E.G. GOVERNMENT) C = FOREIGN-OWNE |
| `dot_certified_disadvantage` | bool | https://www.sam.gov | F = False T = True |
| `economically_disadvantaged` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is an Economically Disadvantaged Woman Owned Small Business or not. It can be derived from the SAM  | F = False T = True |
| `emerging_small_business` | bool | List characteristic of the contractor such as whether the selected contractor is an Emerging Small Business Organization or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `for_profit_organization` | bool | List characteristic of the contractor such as whether the selected contractor is a Profit Organization or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `foreign_owned_and_located` | bool | https://www.sam.gov | F = False T = True |
| `hispanic_american_owned_bu` | bool | List characteristic of the contractor such as whether the selected contractor is a Hispanic American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `joint_venture_economically` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Joint Venture Economically Disadvantaged Woman Owned Small Business or not. It can be derived  | F = False T = True |
| `joint_venture_women_owned` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Joint Venture Woman Owned Small Business or not. It can be derived from the SAM data element,  | F = False T = True |
| `labor_surplus_area_firm` | bool | https://www.sam.gov | F = False T = True |
| `local_government_owned` | bool | https://www.sam.gov | F = False T = True |
| `minority_institution` | bool | List characteristic of the contractor such as whether the selected contractor is a Minority Institution or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `minority_owned_business` | bool | https://www.sam.gov | F = False T = True |
| `native_american_owned_busi` | bool | List characteristic of the contractor such as whether the selected contractor is a Native American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `native_hawaiian_owned_busi` | bool | https://www.sam.gov | F = False T = True |
| `native_hawaiian_servicing` | bool | https://www.sam.gov | F = False T = True |
| `nonprofit_organization` | bool | List characteristic of the contractor such as whether the selected contractor is a Nonprofit Organization or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `other_minority_owned_busin` | bool | https://www.sam.gov | F = False T = True |
| `other_not_for_profit_organ` | bool | https://www.sam.gov | F = False T = True |
| `sba_certified_8_a_joint_ve` | bool | https://www.sam.gov | F = False T = True |
| `self_certified_small_disad` | bool | https://www.sam.gov | F = False T = True |
| `service_disabled_veteran_o` | bool | List characteristic of the contractor such as whether the selected contractor is a Service-Related Disabled Veteran Owned Business or not. It can be derived from the SAM data element, 'Business Types' | F = False T = True |
| `small_business_competitive` | bool | Indicates whether the contract was awarded to a U.S. business concern as a result of a solicitation issued on or after Jan 1, 1989 for the four designated industry groups or the ten targeted industry  | Y = Yes N = No |
| `small_disadvantaged_busine` | bool | List characteristic of the contractor such as whether the selected contractor is a Small Disadvantaged Business Organization or not. It can be derived from the SAM data element, 'Business Types'. | Y = Small Disadvantaged Business N = Other than Small Disadvantaged Business |
| `tribal_college` | bool | https://www.sam.gov | F = False T = True |
| `tribally_owned_business` | bool | https://www.sam.gov | F = False T = True |
| `us_tribal_government` | bool | List characteristic of the contractor such as whether the selected contractor is a Tribal Government Organization or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `veteran_owned_business` | bool | List characteristic of the contractor such as whether the selected contractor is a Veteran Owned Business or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `woman_owned_business` | bool | List characteristic of the contractor such as whether the selected contractor is a Woman Owned Business or not. It can be derived from the SAM data element, 'Business Types'. | F = False T = True |
| `women_owned_small_business` | bool | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Woman Owned Small Business or not. It can be derived from the SAM data element, 'Business Type | F = False T = True |

### Place of performance (geography) (15)

| native column | type | definition | domain values |
|---|---|---|---|
| `place_of_perform_zip_last4` | string |  |  |
| `place_of_performance_code` | string | A numeric code indicating where the predominant performance of the award will be accomplished. | 00***** = Multi-State 00FORGN = Foreign XX00000 = Single ZIP code XX##### = City |
| `place_of_performance_forei` | string | For foreign places of performance: identify where the predominant performance of the award will be accomplished, describing it as specifically as possible. |  |
| `place_of_performance_scope` | string | A description of the geographic area to which the predominant performance of the award is applicable. | Multi-State State-wide County-wide City-wide Single ZIP code Foreign |
| `place_of_performance_zip4a` | string | United States ZIP code (five digits) concatenated with the additional +4 digits, identifying where the predominant performance of the award will be accomplished. | Data for validation purposes is sourced from USPS Postal Pro, though agencies ar |
| `pop_city_name` | string |  |  |
| `pop_congressional_code_current` | string |  |  |
| `pop_congressional_population` | int64 |  |  |
| `pop_country_name` | string |  |  |
| `pop_county_code` | string |  |  |
| `pop_county_name` | string |  |  |
| `pop_county_population` | int64 |  |  |
| `pop_state_fips` | string |  |  |
| `pop_state_name` | string |  |  |
| `pop_state_population` | int64 |  |  |

### Funding agency (12)

| native column | type | definition | domain values |
|---|---|---|---|
| `funding_amount` | double |  |  |
| `funding_office_code` | string | Identifier of the level n organization that provided the preponderance of the funds obligated by this transaction. | Refer to the GSA IAE Federal Hierarchy from SAM.gov |
| `funding_office_name` | string | Name of the level n organization that provided the preponderance of the funds obligated by this transaction. | Refer to the GSA IAE Federal Hierarchy from SAM.gov |
| `funding_opportunity_goals` | string | A brief summary of the intended outcomes associated with the notice of funding opportunity. Applicable to Competitive Discretionary Grants and Cooperative Agreements. |  |
| `funding_opportunity_number` | string | An alphanumeric identifier that a Federal agency assigns to its funding opportunity announcement as part of the Notice of Funding Opportunity posted on the OMB-designated government wide web site (cur |  |
| `funding_sub_tier_agency_co` | string | Identifier of the level 2 organization that provided the preponderance of the funds obligated by this transaction. | See https://files.usaspending.gov/reference_data/agency_codes.csv (SUBTIER CODE  |
| `funding_subtier_agency_abbreviation` | string |  |  |
| `funding_subtier_agency_name` | string | Name of the level 2 organization that provided the preponderance of the funds obligated by this transaction. | See https://files.usaspending.gov/reference_data/agency_codes.csv (SUBTIER NAME  |
| `funding_subtier_agency_name_raw` | string |  |  |
| `funding_toptier_agency_abbreviation` | string |  |  |
| `funding_toptier_agency_id` | int64 |  |  |
| `funding_toptier_agency_name_raw` | string |  |  |

### Parent / IDV / vehicle (11)

| native column | type | definition | domain values |
|---|---|---|---|
| `idv_type_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the IDV_Type Field. | A = GWAC B = IDC C = FSS D = BOA E = BPA |
| `multiple_or_single_aw_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multiple or Single Award IDV Field. | M = MULTIPLE AWARD S = SINGLE AWARD |
| `multiple_or_single_award_i` | string | Indicates whether the contract is one of many that resulted from a single solicitation, all of the contracts are for the same or similar items, and contracting officers are required to compare their r | M = MULTIPLE AWARD S = SINGLE AWARD |
| `ordering_period_end_date` | string | For procurement, the date on which, for the award referred to by the action being reported, no additional orders referring to it may be placed. This date applies only to procurement indefinite deliver |  |
| `parent_recipient_name_raw` | string |  |  |
| `parent_recipient_unique_id` | string |  |  |
| `referenced_idv_agency_desc` | string | Name of the agency associated with the code in the Referenced IDV Agency Identifier. | Refer to the GSA Federal Procurement Data System (FPDS) |
| `referenced_idv_agency_iden` | string | Identifier used to link agency in FPDS to referenced IDV information. | Refer to the GSA Federal Procurement Data System (FPDS) |
| `referenced_idv_modificatio` | string | When reporting orders under Indefinite Delivery Vehicles (IDV) such as a GWAC, IDC, FSS, BOA, or BPA, report the Modification Number along with Procurement Instrument Identifier (Contract Number or Ag |  |
| `referenced_idv_type` | string | The type of Indefinite Delivery Vehicle (IDV) being loaded by the IDV referenced in this transaction. Referenced IDV Types include Government-Wide Acquisition Contract (GWAC), Multi-Agency Contract, O | A = GWAC B = IDC C = FSS D = BOA E = BPA |
| `referenced_idv_type_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Referenced_IDV_Type Field. | A = GWAC B = IDC C = FSS D = BOA E = BPA |

### Competition & set-aside (17)

| native column | type | definition | domain values |
|---|---|---|---|
| `commercial_item_acqui_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Commercial Item Acquisition Procedures Field. | A = COMMERCIAL ITEM B = SUPPLIES OR SERVICES PURSUANT TO FAR 12.102(F) C = SERVI |
| `commercial_item_acquisitio` | string | Designates whether the solicitation used the special requirements for the acquisition of commercial items (or other supplies or services authorized to use commercial item procedures) intended to more  | A = COMMERCIAL ITEM B = SUPPLIES OR SERVICES PURSUANT TO FAR 12.102(F) C = SERVI |
| `commercial_item_test_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Commercial Item Test Program Field. | Y = Yes N = No |
| `commercial_item_test_progr` | string | This field designates whether the acquisition utilized FAR 13.5 Test Program for Certain Commercial Items. The FAR 13.5 Test Program provides for the use of simplified acquisition procedures for the a | Y = YES N = NO |
| `evaluated_preference` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Evaluated Preference Field. | NONE = NO PREFERENCE USED SDA = SDB PRICE EVALUATION ADJUSTMENT SPS = SDB PREFER |
| `evaluated_preference_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Evaluated Preference Field. | NONE = NO PREFERENCE USED SDA = SDB PRICE EVALUATION ADJUSTMENT SPS = SDB PREFER |
| `extent_compete_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Extent Competed Field. | A = FULL AND OPEN COMPETITION B = NOT AVAILABLE FOR COMPETITION C = NOT COMPETED |
| `fair_opportunity_limi_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Fair Opportunity Limited Sources Field. | URG = URGENCY ONE = ONLY ONE SOURCE - OTHER FOO = FOLLOW-ON ACTION FOLLOWING COM |
| `fair_opportunity_limited_s` | string | The type of statutory exception to Fair Opportunity. | URG = URGENCY ONE = ONLY ONE SOURCE - OTHER FOO = FOLLOW-ON ACTION FOLLOWING COM |
| `local_area_set_aside` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Local Area Set Aside Field. | Y = YES N = NO |
| `local_area_set_aside_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Local Area Set Aside Field. | Y = YES N = NO |
| `number_of_offers_received` | string | The number of actual offers/bids received in response to the solicitation. |  |
| `other_than_full_and_o_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Other than Full and Open Competition Field. | UNQ = UNIQUE SOURCE (FAR 6.302-1(B)(1)) FOC = FOLLOW-ON CONTRACT (FAR 6.302-1(A) |
| `other_than_full_and_open_c` | string | The designator for solicitation procedures other than full and open competition pursuant to FAR 6.3. | UNQ = UNIQUE SOURCE (FAR 6.302-1(B)(1)) FOC = FOLLOW-ON CONTRACT (FAR 6.302-1(A) |
| `price_evaluation_adjustmen` | string | The percent difference between the award price and the lowest priced offer from a responsive, responsible non-HUBZone or non-SDB. |  |
| `solicitation_procedures` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Solicitation Procedures Field. | NP = NEGOTIATED PROPOSAL/QUOTE SB = SEALED BID TS = TWO STEP SP1 = SIMPLIFIED AC |
| `type_set_aside_description` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type Set Aside Field. | NONE = NO SET ASIDE USED SBA = SMALL BUSINESS SET ASIDE - TOTAL 8A = 8A COMPETED |

### Contract characteristics (27)

| native column | type | definition | domain values |
|---|---|---|---|
| `clinger_cohen_act_pla_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Clinger-Cohen Act Planning Compliance Field. | Y = Yes N = No |
| `clinger_cohen_act_planning` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Clinger-Cohen Act Planning Compliance Field. | Y = Yes N = No |
| `consolidated_contract` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Consolidated Contract Field. | A = CONSOLIDATED REQUIREMENTS B = CONSOLIDATED REQUIREMENTS WITH WRITTEN DETERMI |
| `consolidated_contract_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Consolidated Contract Field. | A = CONSOLIDATED REQUIREMENTS B = CONSOLIDATED REQUIREMENTS WITH WRITTEN DETERMI |
| `contract_bundling` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Bundling Field. | A = MISSION CRITICAL B = OMB Circular A-76 C = OTHER D = NOT A BUNDLED REQUIREME |
| `contract_bundling_descrip` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Bundling Field. | A = MISSION CRITICAL B = OMB Circular A-76 C = OTHER D = NOT A BUNDLED REQUIREME |
| `contract_financing` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Financing Field. | A = FAR 52.232-16 PROGRESS PAYMENTS C = PERCENTAGE OF COMPLETION PROGRESS PAYMEN |
| `contract_financing_descrip` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Financing Field. | A = FAR 52.232-16 PROGRESS PAYMENTS C = PERCENTAGE OF COMPLETION PROGRESS PAYMEN |
| `cost_or_pricing_data` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost or Pricing Data Field. | Y = YES N = NO W = NOT OBTAINED - WAIVED |
| `cost_or_pricing_data_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost or Pricing Data Field. | Y = YES N = NO W = NOT OBTAINED - WAIVED |
| `dod_claimant_prog_cod_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the DoD Claimant Program Code Field. | According to the GSA Federal Procurement Data System (FPDS), these are listed in |
| `dod_claimant_program_code` | string | A claimant program number designates a grouping of supplies, construction, or other services. | According to the GSA Federal Procurement Data System (FPDS), these are listed in |
| `inherently_government_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Inherently Governmental Functions field. | CL = CLOSELY ASSOCIATED CT = CRITICAL FUNCTIONS OT = OTHER FUNCTIONS CL,CT = CLO |
| `inherently_government_func` | string | Indicates the type of the "Inherently Governmental Function" used on the action. | CL = CLOSELY ASSOCIATED CT = CRITICAL FUNCTIONS OT = OTHER FUNCTIONS CL,CT = CLO |
| `major_program` | string | The agency determined code for a major program within the agency. For an Indefinite Delivery Vehicle, this may be the name of a GWAC (e.g., ITOPS or COMMITS). |  |
| `multi_year_contract` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multi Year Contract Field. | Y = YES N = NO |
| `multi_year_contract_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multi Year Contract Field. | Y = YES N = NO |
| `national_interest_action` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the National Interest Action Field. | NONE = NONE H05K = HURRICANE KATRINA 2005 H05O = HURRICANE OPHELIA 2005 H05R = H |
| `national_interest_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the National Interest Action Field. | NONE = NONE H05K = HURRICANE KATRINA 2005 H05O = HURRICANE OPHELIA 2005 H05R = H |
| `performance_based_se_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Performance-Based Service Acquisition Field. | Y = YES - SERVICE WHERE PBA IS USED. N = NO - SERVICE WHERE PBA IS NOT USED. X = |
| `performance_based_service` | string | Indicates whether the contract action is a PBA of services as defined by FAR 37.601. A PBSA: a. Describes the requirements in terms of results required rather than the methods of performance of the wo | Y = YES - SERVICE WHERE PBA IS USED. N = NO - SERVICE WHERE PBA IS NOT USED. X = |
| `program_acronym` | string | The short name or title used for a GWAC or other contracting program. Examples include COMMITS, ITOPS, SEWP. |  |
| `purchase_card_as_paym_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Purchase Card as Payment Method Field. | Y = YES N = NO |
| `purchase_card_as_payment_m` | string | Indicates whether the method of payment is the Purchase Card. Agencies may issue formal contract documents and make payment using the Purchase Card. It is also permitted that agencies may report Purch | Y = YES N = NO |
| `type_of_contract_pric_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the TypeOfContractPricing Field. | A = FIXED PRICE REDETERMINATION B = FIXED PRICE LEVEL OF EFFORT J = FIRM FIXED P |
| `undefinitized_action` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Undefinitized Action Field. | A = LETTER CONTRACT B = OTHER UNDEFINITIZED ACTION X = NO |
| `undefinitized_action_desc` | string | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Undefinitized Action Field. | A = LETTER CONTRACT B = OTHER UNDEFINITIZED ACTION X = NO |

## Full bulk inventory by USAspending grouping


### Account Status (1)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `disaster_emergency_fund_codes` | string | disaster_emergency_fund_codes | Distinguishes whether the budgetary resources, obligations incurred, unobligated and obligated balances, and outlays are classified as disaster, emergency, wildfire suppression or none of the three. ( |

### Award Attribute (148)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `action_date` | date32[day] | action_date | The date the action being reported was issued / signed by the Government or a binding agreement was reached. |
| ✅ IN | `action_type` | string | action_type_code | Code that provides information on any new (only applicable to financial assistance awards) or changes (applies to both procurement and financial assistance changes) made to the Federal prime award. Th |
| ✅ IN | `award_id` | int64 | award_id | The unique identifying Award ID of the prime award (PIID or FAIN). |
| ✅ IN | `cfda_number` | string | cfda_number | The number assigned to an Assistance Listing in the Catalog of Federal Domestic Assistance (CFDA) and SAM.gov. |
| ✅ IN | `construction_wage_rate_req` | string | construction_wage_rate_requirements_code | Indicates whether the transaction is subject to the Construction Wage Rate Requirements. The clause is 52.222-6 "Construction Wage Rate Requirements" -that goes with Wage Rate Requirements (Constructi |
| ✅ IN | `contract_award_type` | string | award_type_code | The type of award being entered by this transaction. Types of awards include Purchase Orders (PO), Delivery Orders (DO), Blanket Purchase Agreements (BPA) Calls and Definitive Contracts. |
| ✅ IN | `detached_award_proc_unique` | string | contract_transaction_unique_key | Derived element and system-generated database key used to uniquely identify each contract transaction record and facilitate record lookup, correction, and deletion. A concatenation of agencyID, Refere |
| ✅ IN | `extent_competed` | string | extent_competed | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Extent Competed Field. |
| ✅ IN | `fiscal_year` | int64 | action_date_fiscal_year | The fiscal year in which the ActionDate occurs. Note that the Federal fiscal year begins on October 1 and ends on September 30, thus October 1, 2018 is the first day of the 2019 fiscal year. |
| ✅ IN | `generated_unique_award_id` | string | contract_award_unique_key | Derived unique record key used by the Broker to identify the prime award. Note that this element is different from the AssistanceTransactionUniqueKey and the ContractTransactionUniqueKey in that it id |
| ✅ IN | `idv_type` | string | idv_type_code | The type of Indefinite Delivery Vehicle being (IDV) loaded by this transaction. IDV Types include Government-Wide Acquisition Contract (GWAC), Multi-Agency Contract, Other Indefinite Delivery Contract |
| ✅ IN | `labor_standards` | string | labor_standards_code | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Service Contract Labor Standards Field. |
| ✅ IN | `last_modified_date` | timestamp[us] | last_modified_date | The last modified date captures the change date. |
| ✅ IN | `modification_number` | string | modification_number | The identifier of an action being reported that indicates the specific subsequent change to the initial award. |
| ✅ IN | `naics_code` | string | naics_code | The identifier that represents the North American Industrial Classification System (NAICS) Code assigned to the solicitation and resulting award identifying the industry in which the contract requirem |
| ✅ IN | `naics_description` | string | naics_description | The title associated with the NAICS Code. |
| ✅ IN | `parent_award_id` | string | parent_award_id_piid | The identifier of the procurement award under which the specific award is issued, such as a Federal Supply Schedule. This data element currently applies to procurement actions only. |
| ✅ IN | `period_of_performance_current_end_date` | date32[day] | period_of_performance_current_end_date | For procurement awards: The contract completion date based on the schedule in the contract. For an initial award, this is the scheduled completion date for the base contract and for any options exerci |
| ✅ IN | `period_of_performance_start_date` | date32[day] | period_of_performance_start_date | For procurement awards: Per the FPDS data dictionary, the date that the parties agree will be the starting date for the contract's requirements. This is the period of performance start date for the en |
| ✅ IN | `piid` | string | award_id_piid | The unique identifier of the specific award being reported. |
| ✅ IN | `pop_country_code` | string | primary_place_of_performance_country_code | Country code where the predominant performance of the award will be accomplished. |
| ✅ IN | `pop_state_code` | string | primary_place_of_performance_state_code | United States Postal Service (USPS) two-letter abbreviation for the state or territory indicating where the predominant performance of the award will be accomplished. Identify States, the District of  |
| ✅ IN | `pop_zip5` | string | primary_place_of_performance_zip_4 | United States ZIP code (five digits) concatenated with the additional +4 digits, identifying where the predominant performance of the award will be accomplished. |
| ✅ IN | `product_or_service_code` | string | product_or_service_code | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Product or Service Code Field. |
| ✅ IN | `product_or_service_description` | string | product_or_service_code_description | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Product or Service Code Field. |
| ✅ IN | `solicitation_identifier` | string | solicitation_identifier | Identifier used to link transactions in FPDS to solicitation information. |
| ✅ IN | `subcontracting_plan` | string | subcontracting_plan | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Subcontracting Plan Field. |
| ✅ IN | `transaction_description` | string | transaction_description | For procurement awards: Per the FPDS data dictionary, a brief, summary level, plain English, description of the contract, award, or modification. Additional information: the description field may also |
| ✅ IN | `transaction_unique_id` | string | contract_transaction_unique_key | Derived element and system-generated database key used to uniquely identify each contract transaction record and facilitate record lookup, correction, and deletion. A concatenation of agencyID, Refere |
| ✅ IN | `type_of_contract_pricing` | string | type_of_contract_pricing_code | The type of contract as defined in FAR Part 16 that applies to this procurement. |
| ✅ IN | `type_set_aside` | string | type_of_set_aside_code | The designator for type of set aside determined for the contract action. |
| ⬜ out | `a_76_fair_act_action` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the A-76 FAIR Act Action Field. |
| ⬜ out | `a_76_fair_act_action_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the A-76 FAIR Act Action Field. |
| ⬜ out | `action_type_description` | string |  | Description tag that explains the meaning of the code provided in the ActionType Field. |
| ⬜ out | `afa_generated_unique` | string |  | System-generated database key used to uniquely identify each financial assistance transaction record and facilitate record lookup, correction, and deletion. A concatenation of AwardingSubTierAgencyCod |
| ⬜ out | `business_funds_ind_desc` | string |  | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the BusinessFundsIndicator Field. |
| ⬜ out | `business_funds_indicator` | string |  | The Business Funds Indicator sometimes abbreviated BFI. Code indicating the award's applicability to the Recovery Act. |
| ⬜ out | `cfda_title` | string |  | The title of the Assistance Listing under which the Federal award was funded in the Catalog of Federal Domestic Assistance (CFDA) and SAM.gov. |
| ➕ PROPOSE | `clinger_cohen_act_pla_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Clinger-Cohen Act Planning Compliance Field. |
| ➕ PROPOSE | `clinger_cohen_act_planning` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Clinger-Cohen Act Planning Compliance Field. |
| ➕ PROPOSE | `commercial_item_acqui_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Commercial Item Acquisition Procedures Field. |
| ➕ PROPOSE | `commercial_item_acquisitio` | string |  | Designates whether the solicitation used the special requirements for the acquisition of commercial items (or other supplies or services authorized to use commercial item procedures) intended to more  |
| ➕ PROPOSE | `commercial_item_test_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Commercial Item Test Program Field. |
| ➕ PROPOSE | `commercial_item_test_progr` | string |  | This field designates whether the acquisition utilized FAR 13.5 Test Program for Certain Commercial Items. The FAR 13.5 Test Program provides for the use of simplified acquisition procedures for the a |
| ➕ PROPOSE | `consolidated_contract` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Consolidated Contract Field. |
| ➕ PROPOSE | `consolidated_contract_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Consolidated Contract Field. |
| ⬜ out | `construction_wage_rat_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Wage Rate Requirements (Construction) Field. |
| ⬜ out | `contingency_humanitar_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Emergency Acquisition Field. |
| ⬜ out | `contingency_humanitarian_o` | string |  | A designator of contract actions that support a declared contingency operation, a declared humanitarian or peacekeeping operation, or a declared presidential issued emergency declaration or a major di |
| ⬜ out | `contract_award_type_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the ContractAwardType Field. |
| ➕ PROPOSE | `contract_bundling` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Bundling Field. |
| ➕ PROPOSE | `contract_bundling_descrip` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Bundling Field. |
| ➕ PROPOSE | `contract_financing` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Financing Field. |
| ➕ PROPOSE | `contract_financing_descrip` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contract Financing Field. |
| ⬜ out | `correction_delete_ind_desc` | string |  | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the CorrectionDeleteIndicator Field. |
| ⬜ out | `correction_delete_indicatr` | string |  | A code to indicate how the record should be processed: correction to an existing record; deletion of a record; new record. |
| ⬜ out | `cost_accounting_stand_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost Accounting Standards Clause Field. |
| ⬜ out | `cost_accounting_standards` | string |  | Indicates whether the contract includes a Cost Accounting Standards clause. |
| ➕ PROPOSE | `cost_or_pricing_data` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost or Pricing Data Field. |
| ➕ PROPOSE | `cost_or_pricing_data_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Cost or Pricing Data Field. |
| ⬜ out | `country_of_product_or_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Country of Product or Service Origin Field. |
| ⬜ out | `country_of_product_or_serv` | string |  | Identifies the country of product or service origin. |
| ➕ PROPOSE | `dod_claimant_prog_cod_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the DoD Claimant Program Code Field. |
| ➕ PROPOSE | `dod_claimant_program_code` | string |  | A claimant program number designates a grouping of supplies, construction, or other services. |
| ➕ PROPOSE | `domestic_or_foreign_e_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Domestic or Foreign Entity Field. |
| ➕ PROPOSE | `domestic_or_foreign_entity` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Domestic or Foreign Entity Field. |
| ⬜ out | `epa_designated_produc_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the EPA-Designated Product Field. |
| ⬜ out | `epa_designated_product` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the EPA-Designated Product Field. |
| ➕ PROPOSE | `evaluated_preference` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Evaluated Preference Field. |
| ➕ PROPOSE | `evaluated_preference_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Evaluated Preference Field. |
| ➕ PROPOSE | `extent_compete_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Extent Competed Field. |
| ⬜ out | `fain` | string |  | The Federal Award Identification Number (FAIN) is the unique ID within the Federal agency for each (non-aggregate) financial assistance award. |
| ➕ PROPOSE | `fair_opportunity_limi_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Fair Opportunity Limited Sources Field. |
| ➕ PROPOSE | `fair_opportunity_limited_s` | string |  | The type of statutory exception to Fair Opportunity. |
| ⬜ out | `fed_biz_opps` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the FedBizOpps Field. |
| ⬜ out | `fed_biz_opps_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the FedBizOpps Field. |
| ⬜ out | `government_furnished_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Government Furnished Property GFP Field. |
| ⬜ out | `government_furnished_prope` | string |  | The contract uses equipment or property furnished by the government, pursuant to FAR 45. |
| ➕ PROPOSE | `idv_type_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the IDV_Type Field. |
| ⬜ out | `information_technolog_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Information Technology Commercial Item Category Field. |
| ⬜ out | `information_technology_com` | string |  | A code that designates the commercial availability of an information technology product or service. |
| ➕ PROPOSE | `inherently_government_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Inherently Governmental Functions field. |
| ➕ PROPOSE | `inherently_government_func` | string |  | Indicates the type of the "Inherently Governmental Function" used on the action. |
| ⬜ out | `interagency_contract_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Interagency Contracting Authority Field. |
| ⬜ out | `interagency_contracting_au` | string |  | Indicates whether the transaction is an Economy Act or Statutory Authority. |
| ⬜ out | `labor_standards_descrip` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Service Contract Labor Standards Field. |
| ➕ PROPOSE | `local_area_set_aside` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Local Area Set Aside Field. |
| ➕ PROPOSE | `local_area_set_aside_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Local Area Set Aside Field. |
| ➕ PROPOSE | `major_program` | string |  | The agency determined code for a major program within the agency. For an Indefinite Delivery Vehicle, this may be the name of a GWAC (e.g., ITOPS or COMMITS). |
| ⬜ out | `materials_supplies_article` | string |  | Indicates whether the transaction is subject to the Materials, Supplies, Articles, & Equip. The clause is 52.222-20 "Contracts for Materials, Supplies, Articles, and Equipment Exceeding $15,000" - tha |
| ⬜ out | `materials_supplies_descrip` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contracts for Materials, Supplies, Articles, and Equipment Exceeding $15,000 Field. |
| ➕ PROPOSE | `multi_year_contract` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multi Year Contract Field. |
| ➕ PROPOSE | `multi_year_contract_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multi Year Contract Field. |
| ➕ PROPOSE | `multiple_or_single_aw_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Multiple or Single Award IDV Field. |
| ➕ PROPOSE | `multiple_or_single_award_i` | string |  | Indicates whether the contract is one of many that resulted from a single solicitation, all of the contracts are for the same or similar items, and contracting officers are required to compare their r |
| ➕ PROPOSE | `national_interest_action` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the National Interest Action Field. |
| ➕ PROPOSE | `national_interest_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the National Interest Action Field. |
| ⬜ out | `number_of_actions` | string |  | The number input by the agency that identifies number of actions that are reported in one modification. |
| ➕ PROPOSE | `number_of_offers_received` | string |  | The number of actual offers/bids received in response to the solicitation. |
| ➕ PROPOSE | `ordering_period_end_date` | string |  | For procurement, the date on which, for the award referred to by the action being reported, no additional orders referring to it may be placed. This date applies only to procurement indefinite deliver |
| ⬜ out | `other_statutory_authority` | string |  | Indicates whether the transaction is subject to other statutory authority. If "Interagency Contracting Authority" is "Other Statutory Authority" then an entry is required in this data element. |
| ➕ PROPOSE | `other_than_full_and_o_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Other than Full and Open Competition Field. |
| ➕ PROPOSE | `other_than_full_and_open_c` | string |  | The designator for solicitation procedures other than full and open competition pursuant to FAR 6.3. |
| ➕ PROPOSE | `performance_based_se_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Performance-Based Service Acquisition Field. |
| ➕ PROPOSE | `performance_based_service` | string |  | Indicates whether the contract action is a PBA of services as defined by FAR 37.601. A PBSA: a. Describes the requirements in terms of results required rather than the methods of performance of the wo |
| ⬜ out | `period_of_perf_potential_e` | string |  | For procurement, the date on which, for the award referred to by the action being reported if all potential pre-determined or pre-negotiated options were exercised, awardee effort is completed or the  |
| ⬜ out | `place_of_manufacture` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Place of Manufacture Field. |
| ⬜ out | `place_of_manufacture_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Place of Manufacture Field. |
| ➕ PROPOSE | `place_of_performance_code` | string |  | A numeric code indicating where the predominant performance of the award will be accomplished. |
| ➕ PROPOSE | `place_of_performance_forei` | string |  | For foreign places of performance: identify where the predominant performance of the award will be accomplished, describing it as specifically as possible. |
| ➕ PROPOSE | `place_of_performance_scope` | string |  | A description of the geographic area to which the predominant performance of the award is applicable. |
| ➕ PROPOSE | `place_of_performance_zip4a` | string |  | United States ZIP code (five digits) concatenated with the additional +4 digits, identifying where the predominant performance of the award will be accomplished. |
| ➕ PROPOSE | `price_evaluation_adjustmen` | string |  | The percent difference between the award price and the lowest priced offer from a responsive, responsible non-HUBZone or non-SDB. |
| ➕ PROPOSE | `program_acronym` | string |  | The short name or title used for a GWAC or other contracting program. Examples include COMMITS, ITOPS, SEWP. |
| ⬜ out | `program_system_or_equ_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the DOD Acquisition Program field. |
| ⬜ out | `program_system_or_equipmen` | string |  | Two codes that together identify the program and weapons system or equipment purchased by a DoD agency. The first character is a number 1-4 that identifies the DoD component. The last 3 characters ide |
| ⬜ out | `pulled_from` | string |  | Flag indicating whether the record was pulled from the award Atom feed or the IDV (Indefinite Delivery Vehicle) Atom Feed provided by FPDS. |
| ➕ PROPOSE | `purchase_card_as_paym_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Purchase Card as Payment Method Field. |
| ➕ PROPOSE | `purchase_card_as_payment_m` | string |  | Indicates whether the method of payment is the Purchase Card. Agencies may issue formal contract documents and make payment using the Purchase Card. It is also permitted that agencies may report Purch |
| ⬜ out | `record_type` | int64 |  | Code indicating whether an action is an aggregate record, a non-aggregate record, or a non-aggregate record to an individual recipient (PII-Redacted). |
| ⬜ out | `record_type_description` | string |  | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the RecordType Field. |
| ⬜ out | `recovered_materials_s_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Recovered Materials/Sustainability Field. |
| ⬜ out | `recovered_materials_sustai` | string |  | Designates whether Recovered Material Certification and/or Estimate of Percentage of Recovered Material Content for EPA-Designated Products clauses were included in the contract. |
| ➕ PROPOSE | `referenced_idv_agency_desc` | string |  | Name of the agency associated with the code in the Referenced IDV Agency Identifier. |
| ➕ PROPOSE | `referenced_idv_agency_iden` | string |  | Identifier used to link agency in FPDS to referenced IDV information. |
| ➕ PROPOSE | `referenced_idv_modificatio` | string |  | When reporting orders under Indefinite Delivery Vehicles (IDV) such as a GWAC, IDC, FSS, BOA, or BPA, report the Modification Number along with Procurement Instrument Identifier (Contract Number or Ag |
| ➕ PROPOSE | `referenced_idv_type` | string |  | The type of Indefinite Delivery Vehicle (IDV) being loaded by the IDV referenced in this transaction. Referenced IDV Types include Government-Wide Acquisition Contract (GWAC), Multi-Agency Contract, O |
| ➕ PROPOSE | `referenced_idv_type_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Referenced_IDV_Type Field. |
| ⬜ out | `referenced_mult_or_si_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Referenced IDV Multiple or Single Field. |
| ⬜ out | `referenced_mult_or_single` | string |  | Indicates whether the contract of the referenced IDV is one of many that resulted from a single solicitation, all of the contracts are for the same or similar items, and contracting officers are requi |
| ⬜ out | `research` | string |  | The designator for type of research determined for the contract action. |
| ⬜ out | `research_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Research Field. |
| ⬜ out | `sai_number` | string |  | A number assigned by state (as opposed to federal) review agencies to the award during the grant application process. |
| ⬜ out | `sea_transportation` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Sea Transportation Field. |
| ⬜ out | `sea_transportation_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Sea Transportation Field. |
| ➕ PROPOSE | `small_business_competitive` | bool |  | Indicates whether the contract was awarded to a U.S. business concern as a result of a solicitation issued on or after Jan 1, 1989 for the four designated industry groups or the ten targeted industry  |
| ⬜ out | `solicitation_date` | date32[day] |  | For award of a new contract, purchase order, task\delivery order, or BPA Call valued above the SAT a solicitation issuance date must be provided, regardless of whether the new award: Was required to b |
| ⬜ out | `solicitation_procedur_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Solicitation Procedures Field. |
| ➕ PROPOSE | `solicitation_procedures` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Solicitation Procedures Field. |
| ⬜ out | `subcontracting_plan_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Subcontracting Plan Field. |
| ⬜ out | `transaction_number` | string |  | Tie Breaker for legal, unique transactions that would otherwise have the same key. |
| ➕ PROPOSE | `type_of_contract_pric_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the TypeOfContractPricing Field. |
| ⬜ out | `type_of_idc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type of IDC Field. |
| ⬜ out | `type_of_idc_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type of IDC Field. |
| ➕ PROPOSE | `type_set_aside_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Type Set Aside Field. |
| ➕ PROPOSE | `undefinitized_action` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Undefinitized Action Field. |
| ➕ PROPOSE | `undefinitized_action_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Undefinitized Action Field. |
| ⬜ out | `uri` | string |  | Unique Record Identifier. An agency defined identifier that (when provided) is unique for every financial assistance action reported by that agency. USAspending.gov and the Broker use URI as the Award |

### Award Recipient (121)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `business_types` | string | business_types | A code of a collection of indicators of different types of recipients based on socio-economic status and organization / business areas. |
| ✅ IN | `cage_code` | string | cage_code | The CAGE Code of the entity. Used as a key to SAM. Maps to the Unique Entity ID. |
| ✅ IN | `contracting_officers_deter` | string | contracting_officers_determination_of_business_size | The Contracting Officer's determination of whether the selected contractor meets the small business size standard for award to a small business for the NAICS code that is applicable to the contract. |
| ✅ IN | `legal_entity_address_line1` | string | recipient_address_line_1 | First line of the awardee or recipient’s legal business address where the office represented by the Unique Entity Identifier (as registered in the System for Award Management) is located. |
| ✅ IN | `recipient_location_city_name` | string | recipient_city_name | Name of the city in which the awardee or recipient’s legal business address is located. |
| ✅ IN | `recipient_location_country_code` | string | recipient_country_code | Code for the country in which the awardee or recipient is located, using the International Standard for country codes (ISO) 3166-1 Alpha-3 GENC Profile, minus the codes listed for those territories an |
| ✅ IN | `recipient_location_county_name` | string | recipient_county_name | Name of the county in which the awardee or recipient’s legal business address is located. |
| ✅ IN | `recipient_location_state_code` | string | recipient_state_code | United States Postal Service (USPS) two-letter abbreviation for the state or territory in which the awardee or recipient’s legal business address is located. Identify States, the District of Columbia, |
| ✅ IN | `recipient_name` | string | recipient_name | The name of the awardee or recipient that relates to the unique identifier. For U.S. based companies, this name is what the business ordinarily files in formation documents with individual states (whe |
| ⬜ out | `airport_authority` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `alaskan_native_owned_corpo` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `alaskan_native_servicing_i` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `american_indian_owned_busi` | bool |  | List characteristic of the contractor such as whether the selected contractor is an American Indian Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `asian_pacific_american_own` | bool |  | List characteristic of the contractor such as whether the selected contractor is an Asian-Pacific American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ➕ PROPOSE | `black_american_owned_busin` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Black American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `business_types_desc` | string |  | Description tag (by way of the DATA Act Broker) that explains the meaning of the code provided in the BusinessType Field. |
| ⬜ out | `c1862_land_grant_college` | bool |  | https://www.sam.gov |
| ⬜ out | `c1890_land_grant_college` | bool |  | https://www.sam.gov |
| ⬜ out | `c1994_land_grant_college` | bool |  | https://www.sam.gov |
| ⬜ out | `c8a_program_participant` | bool |  | List characteristic of the contractor such as whether the selected contractor is an 8(a) Program Participant Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `city_local_government` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `community_developed_corpor` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `community_development_corp` | bool |  | https://www.sam.gov |
| ⬜ out | `contracting_officers_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Contracting Officer's Determination of Business Size Field. |
| ⬜ out | `contracts` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `corporate_entity_not_tax_e` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `corporate_entity_tax_exemp` | bool |  | https://www.sam.gov |
| ⬜ out | `council_of_governments` | bool |  | https://www.sam.gov |
| ⬜ out | `county_local_government` | bool |  | https://www.sam.gov |
| ⬜ out | `domestic_shelter` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `dot_certified_disadvantage` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `economically_disadvantaged` | bool |  | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is an Economically Disadvantaged Woman Owned Small Business or not. It can be derived from the SAM  |
| ⬜ out | `educational_institution` | bool |  | List characteristic of the contractor such as whether the selected contractor is an Educational Institution or not. It can be derived from the SAM data element, 'Business Types'. |
| ➕ PROPOSE | `emerging_small_business` | bool |  | List characteristic of the contractor such as whether the selected contractor is an Emerging Small Business Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `federal_agency` | bool |  | https://www.sam.gov |
| ⬜ out | `federally_funded_research` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `for_profit_organization` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Profit Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `foreign_government` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `foreign_owned_and_located` | bool |  | https://www.sam.gov |
| ⬜ out | `foundation` | bool |  | https://www.sam.gov |
| ⬜ out | `grants` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `hispanic_american_owned_bu` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Hispanic American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `hispanic_servicing_institu` | bool |  | https://www.sam.gov |
| ⬜ out | `historically_black_college` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Historically Black College or University or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `historically_underutilized` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Historically Underutilized Business Zone (HUBZone) Firm or not. It can be derived from the SAM data element, 'Busines |
| ⬜ out | `hospital_flag` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Hospital or not. It can be derived from the SAM data element, 'Business Types' |
| ⬜ out | `housing_authorities_public` | bool |  | https://www.sam.gov |
| ⬜ out | `indian_tribe_federally_rec` | bool |  | https://www.sam.gov |
| ⬜ out | `inter_municipal_local_gove` | bool |  | https://www.sam.gov |
| ⬜ out | `international_organization` | bool |  | https://www.sam.gov |
| ⬜ out | `interstate_entity` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `joint_venture_economically` | bool |  | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Joint Venture Economically Disadvantaged Woman Owned Small Business or not. It can be derived  |
| ➕ PROPOSE | `joint_venture_women_owned` | bool |  | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Joint Venture Woman Owned Small Business or not. It can be derived from the SAM data element,  |
| ➕ PROPOSE | `labor_surplus_area_firm` | bool |  | https://www.sam.gov |
| ⬜ out | `legal_entity_address_line2` | string |  | Second line of awardee or recipient’s legal business address. |
| ⬜ out | `legal_entity_city_code` | string |  | Five position city code from the validation authoritative list. |
| ⬜ out | `legal_entity_foreign_city` | string |  | For foreign recipients only: name of the city in which the awardee or recipient’s legal business address is located. |
| ⬜ out | `legal_entity_foreign_posta` | string |  | For foreign recipients only: foreign postal code in which the awardee or recipient's legal business address is located. |
| ⬜ out | `legal_entity_foreign_provi` | string |  | For foreign recipients only: name of the state or province in which the awardee or recipient’s legal business address is located. |
| ⬜ out | `legal_entity_zip4` | string |  | USPS zoning code associated with the awardee or recipient’s legal business address. For domestic recipients only. |
| ⬜ out | `legal_entity_zip_last4` | string |  | USPS four digit extension code associated with the awardee or recipient’s legal business address. This must be blank for non-US addresses |
| ⬜ out | `limited_liability_corporat` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `local_government_owned` | bool |  | https://www.sam.gov |
| ⬜ out | `manufacturer_of_goods` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `minority_institution` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Minority Institution or not. It can be derived from the SAM data element, 'Business Types'. |
| ➕ PROPOSE | `minority_owned_business` | bool |  | https://www.sam.gov |
| ⬜ out | `municipality_local_governm` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `native_american_owned_busi` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Native American Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ➕ PROPOSE | `native_hawaiian_owned_busi` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `native_hawaiian_servicing` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `nonprofit_organization` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Nonprofit Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `officer_1_amount` | double |  | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17  |
| ⬜ out | `officer_1_name` | string |  | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| ⬜ out | `officer_2_amount` | double |  | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17  |
| ⬜ out | `officer_2_name` | string |  | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| ⬜ out | `officer_3_amount` | double |  | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17  |
| ⬜ out | `officer_3_name` | string |  | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| ⬜ out | `officer_4_amount` | double |  | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17  |
| ⬜ out | `officer_4_name` | string |  | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| ⬜ out | `officer_5_amount` | double |  | The cash and noncash dollar value earned by the one of the five most highly compensated “Executives” during the awardee's preceding fiscal year and includes the following (for more information see 17  |
| ⬜ out | `officer_5_name` | string |  | The name of an individual identified as one of the five most highly compensated "Executives." "Executive" means officers, managing partners, or any other employees in management positions. |
| ⬜ out | `organizational_type` | string |  | The structure of the entity as defined by the IRS. |
| ➕ PROPOSE | `other_minority_owned_busin` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `other_not_for_profit_organ` | bool |  | https://www.sam.gov |
| ⬜ out | `partnership_or_limited_lia` | bool |  | https://www.sam.gov |
| ⬜ out | `planning_commission` | bool |  | https://www.sam.gov |
| ⬜ out | `port_authority` | bool |  | https://www.sam.gov |
| ⬜ out | `private_university_or_coll` | bool |  | https://www.sam.gov |
| ⬜ out | `receives_contracts_and_gra` | bool |  | https://www.sam.gov |
| ⬜ out | `recipient_name_raw` | string |  | The name of the awardee or recipient that relates to the unique identifier. For U.S. based companies, this name is what the business ordinarily files in formation documents with individual states (whe |
| ⬜ out | `sam_exception` | string |  | The reason a vendor/contractor not registered in the mandated SAM system may be used in a purchase. |
| ⬜ out | `sam_exception_description` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the SAM Exception Field. |
| ➕ PROPOSE | `sba_certified_8_a_joint_ve` | bool |  | https://www.sam.gov |
| ⬜ out | `school_district_local_gove` | bool |  | https://www.sam.gov |
| ⬜ out | `school_of_forestry` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `self_certified_small_disad` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `service_disabled_veteran_o` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Service-Related Disabled Veteran Owned Business or not. It can be derived from the SAM data element, 'Business Types' |
| ⬜ out | `small_agricultural_coopera` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `small_disadvantaged_busine` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Small Disadvantaged Business Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `sole_proprietorship` | bool |  | https://www.sam.gov |
| ⬜ out | `state_controlled_instituti` | bool |  | https://www.sam.gov |
| ⬜ out | `subchapter_s_corporation` | bool |  | https://www.sam.gov |
| ⬜ out | `subcontinent_asian_asian_i` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Subcontinent Asian (Asian- Indian) American Owned Business or not. It can be derived from the SAM data element, 'Busi |
| ⬜ out | `the_ability_one_program` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Sheltered Workshop (JWOD Provider) Organization or not. It can be derived from the SAM data element, 'Business Types' |
| ⬜ out | `township_local_government` | bool |  | https://www.sam.gov |
| ⬜ out | `transit_authority` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `tribal_college` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `tribally_owned_business` | bool |  | https://www.sam.gov |
| ⬜ out | `us_federal_government` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Federal Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `us_government_entity` | bool |  | https://www.sam.gov |
| ⬜ out | `us_local_government` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Local Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `us_state_government` | bool |  | List characteristic of the contractor such as whether the selected contractor is a State Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ➕ PROPOSE | `us_tribal_government` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Tribal Government Organization or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `vendor_doing_as_business_n` | string |  | The doing business as name of the entity address. |
| ⬜ out | `vendor_fax_number` | string |  | The fax number of the entity. |
| ⬜ out | `vendor_phone_number` | string |  | The phone number of the entity. |
| ➕ PROPOSE | `veteran_owned_business` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Veteran Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ⬜ out | `veterinary_college` | bool |  | https://www.sam.gov |
| ⬜ out | `veterinary_hospital` | bool |  | https://www.sam.gov |
| ➕ PROPOSE | `woman_owned_business` | bool |  | List characteristic of the contractor such as whether the selected contractor is a Woman Owned Business or not. It can be derived from the SAM data element, 'Business Types'. |
| ➕ PROPOSE | `women_owned_small_business` | bool |  | https://www.sam.gov OR List characteristic of the contractor such as whether the selected contractor is a Woman Owned Small Business or not. It can be derived from the SAM data element, 'Business Type |

### Award Source (14)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `awarding_agency_code` | string | awarding_agency_code | A department or establishment of the Government as used in the Treasury Account Fund Symbol (TAFS). |
| ✅ IN | `awarding_sub_tier_agency_c` | string | awarding_sub_agency_code | Identifier of the level 2 organization that awarded, executed or is otherwise responsible for the transaction. |
| ✅ IN | `awarding_subtier_agency_name` | string | awarding_sub_agency_name | Name of the level 2 organization that awarded, executed or is otherwise responsible for the transaction. |
| ✅ IN | `awarding_toptier_agency_name` | string | awarding_agency_name | The name associated with a department or establishment of the Government as used in the Treasury Account Fund Symbol (TAFS). |
| ✅ IN | `funding_agency_code` | string | funding_agency_code | The 3-digit CGAC agency code of the department or establishment of the Government that provided the preponderance of the funds for an award and/or individual transactions related to an award. |
| ✅ IN | `funding_toptier_agency_name` | string | funding_agency_name | Name of the department or establishment of the Government that provided the preponderance of the funds for an award and/or individual transactions related to an award. |
| ⬜ out | `awarding_office_code` | string |  | Identifier of the level n organization that awarded, executed or is otherwise responsible for the transaction. |
| ⬜ out | `awarding_office_name` | string |  | Name of the level n organization that awarded, executed or is otherwise responsible for the transaction. |
| ⬜ out | `foreign_funding` | string |  | Indicates that a foreign government, international organization, or foreign military organization bears some of the cost of the acquisition. |
| ⬜ out | `foreign_funding_desc` | string |  | Description tag (by way of the FPDS Atom Feed) that explains the meaning of the code provided in the Foreign Funding Field. |
| ➕ PROPOSE | `funding_office_code` | string |  | Identifier of the level n organization that provided the preponderance of the funds obligated by this transaction. |
| ➕ PROPOSE | `funding_office_name` | string |  | Name of the level n organization that provided the preponderance of the funds obligated by this transaction. |
| ➕ PROPOSE | `funding_sub_tier_agency_co` | string |  | Identifier of the level 2 organization that provided the preponderance of the funds obligated by this transaction. |
| ➕ PROPOSE | `funding_subtier_agency_name` | string |  | Name of the level 2 organization that provided the preponderance of the funds obligated by this transaction. |

### Award Spending (12)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `award_amount` | double | award_amount | The total amount awarded to the prime award recipient. |
| ✅ IN | `base_and_all_options_value` | string | base_and_all_options_value | For the Award it is the mutually agreed upon total contract value including all options (if any). For IDVs the value is the mutually agreed upon total contract value including all options (if any) AND |
| ✅ IN | `current_total_value_award` | string | current_total_value_of_award | For procurement, the total amount obligated to date on a contract, including the base and exercised options. |
| ✅ IN | `federal_action_obligation` | double | federal_action_obligation | Amount of Federal government’s obligation, de-obligation, or liability, in dollars, for an award transaction. |
| ✅ IN | `total_funding_amount` | double | total_funding_amount | The sum of the FederalActionObligation and the Non-Federal Funding Amount. |
| ⬜ out | `base_exercised_options_val` | string |  | The contract value for the base contract and any options that have been exercised. |
| ⬜ out | `face_value_loan_guarantee` | double |  | The face value of the direct loan or loan guarantee. |
| ⬜ out | `indirect_federal_sharing` | double |  | The total amount of any single Federal award action that is allocated, per the award recipient’s approved award budget, to indirect costs. |
| ⬜ out | `non_federal_funding_amount` | double |  | The amount of the award funded by non-Federal source(s), in dollars. Program Income (as defined in 2 CFR § 200.1) is not included until such time that Program Income is generated and credited to the a |
| ⬜ out | `original_loan_subsidy_cost` | double |  | The estimated long-term cost to the Government of a direct loan or loan guarantee, or modification thereof, calculated on a net present value basis, excluding administrative costs. |
| ⬜ out | `potential_total_value_awar` | string |  | For procurement, the total amount that could be obligated on a contract, if the base and all options are exercised. |
| ⬜ out | `total_obligated_amount` | string |  | This is a system generated element providing the sum of all the amounts entered in the "Action Obligation" field for a particular PIID and Agency. Example: Contract has 9 Modifications under "Transact |

### Treasury Account (3)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `federal_accounts` | string | federal_accounts | The Federal Account Symbol is derived from concatenating the agency identifier and the main account code. |
| ✅ IN | `program_activities` | string | program_activities | A single field with associated program activities in order of funding dollars. |
| ⬜ out | `agency_id` | string |  | The agency code identifies the department or agency that is responsible for the account. |

### Uncategorized (3)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `recipient_uei` | string | recipient_uei | The Unique Entity Identifier (UEI) for an awardee or recipient. A UEI is a unique alphanumeric code used to identify a specific commercial, nonprofit, or business entity. |
| ➕ PROPOSE | `funding_opportunity_goals` | string |  | A brief summary of the intended outcomes associated with the notice of funding opportunity. Applicable to Competitive Discretionary Grants and Cooperative Agreements. |
| ➕ PROPOSE | `funding_opportunity_number` | string |  | An alphanumeric identifier that a Federal agency assigns to its funding opportunity announcement as part of the Notice of Funding Opportunity posted on the OMB-designated government wide web site (cur |

### Internal / derived / statistical (76)

| status | native column | type | canonical name | definition |
|---|---|---|---|---|
| ✅ IN | `award_category` | string | award_category |  |
| ✅ IN | `awarding_agency_id` | int64 | awarding_agency_id |  |
| ✅ IN | `awarding_subtier_agency_abbreviation` | string | awarding_subtier_agency_abbreviation |  |
| ✅ IN | `awarding_toptier_agency_abbreviation` | string | awarding_toptier_agency_abbreviation |  |
| ✅ IN | `business_categories` | string | business_categories |  |
| ✅ IN | `funding_agency_id` | int64 | funding_agency_id |  |
| ✅ IN | `parent_recipient_hash` | string | parent_recipient_hash |  |
| ✅ IN | `parent_recipient_name` | string | parent_recipient_name |  |
| ✅ IN | `parent_uei` | string | parent_uei |  |
| ✅ IN | `pop_congressional_code` | string | pop_congressional_code |  |
| ✅ IN | `pop_county_fips` | string | pop_county_fips |  |
| ✅ IN | `recipient_hash` | string | recipient_hash |  |
| ✅ IN | `recipient_levels` | string | recipient_levels |  |
| ✅ IN | `recipient_location_congressional_code` | string | recipient_location_congressional_code |  |
| ✅ IN | `recipient_location_county_fips` | string | recipient_location_county_fips |  |
| ✅ IN | `recipient_location_zip5` | string | recipient_location_zip5 |  |
| ✅ IN | `tas_paths` | string | tas_paths |  |
| ✅ IN | `transaction_id` | int64 | transaction_id |  |
| ✅ IN | `treasury_account_identifiers` | int64 | treasury_account_identifiers |  |
| 🔒 internal | `award_certified_date` | date32[day] |  |  |
| 🔒 internal | `award_date_signed` | date32[day] |  |  |
| 🔒 internal | `award_fiscal_year` | int64 |  |  |
| 🔒 internal | `award_update_date` | timestamp[us] |  |  |
| 🔒 internal | `awarding_subtier_agency_name_raw` | string |  |  |
| 🔒 internal | `awarding_toptier_agency_id` | int64 |  |  |
| 🔒 internal | `awarding_toptier_agency_name_raw` | string |  |  |
| 🔒 internal | `cfda_id` | int64 |  |  |
| 🔒 internal | `create_date` | timestamp[us] |  |  |
| 🔒 internal | `detached_award_procurement_id` | int64 |  |  |
| 🔒 internal | `etl_update_date` | timestamp[us] |  |  |
| 🔒 internal | `fiscal_action_date` | date32[day] |  |  |
| ➕ PROPOSE | `funding_amount` | double |  |  |
| ➕ PROPOSE | `funding_subtier_agency_abbreviation` | string |  |  |
| ➕ PROPOSE | `funding_subtier_agency_name_raw` | string |  |  |
| ➕ PROPOSE | `funding_toptier_agency_abbreviation` | string |  |  |
| ➕ PROPOSE | `funding_toptier_agency_id` | int64 |  |  |
| ➕ PROPOSE | `funding_toptier_agency_name_raw` | string |  |  |
| 🔒 internal | `generated_pragmatic_obligation` | double |  |  |
| 🔒 internal | `ingested_at` | timestamp[us, tz=Etc/UTC] |  |  |
| 🔒 internal | `initial_report_date` | timestamp[us] |  |  |
| 🔒 internal | `is_fpds` | bool |  |  |
| 🔒 internal | `legal_entity_address_line3` | string |  |  |
| 🔒 internal | `legal_entity_foreign_descr` | string |  |  |
| ➕ PROPOSE | `parent_recipient_name_raw` | string |  |  |
| ➕ PROPOSE | `parent_recipient_unique_id` | string |  |  |
| ➕ PROPOSE | `place_of_perform_zip_last4` | string |  |  |
| ➕ PROPOSE | `pop_city_name` | string |  |  |
| ➕ PROPOSE | `pop_congressional_code_current` | string |  |  |
| ➕ PROPOSE | `pop_congressional_population` | int64 |  |  |
| ➕ PROPOSE | `pop_country_name` | string |  |  |
| ➕ PROPOSE | `pop_county_code` | string |  |  |
| ➕ PROPOSE | `pop_county_name` | string |  |  |
| ➕ PROPOSE | `pop_county_population` | int64 |  |  |
| ➕ PROPOSE | `pop_state_fips` | string |  |  |
| ➕ PROPOSE | `pop_state_name` | string |  |  |
| ➕ PROPOSE | `pop_state_population` | int64 |  |  |
| 🔒 internal | `published_fabs_id` | int64 |  |  |
| 🔒 internal | `recipient_location_congressional_code_current` | string |  |  |
| 🔒 internal | `recipient_location_congressional_population` | int64 |  |  |
| 🔒 internal | `recipient_location_country_name` | string |  |  |
| 🔒 internal | `recipient_location_county_code` | string |  |  |
| 🔒 internal | `recipient_location_county_population` | int64 |  |  |
| 🔒 internal | `recipient_location_state_fips` | string |  |  |
| 🔒 internal | `recipient_location_state_name` | string |  |  |
| 🔒 internal | `recipient_location_state_population` | int64 |  |  |
| 🔒 internal | `recipient_unique_id` | string |  |  |
| 🔒 internal | `source_schema` | string |  |  |
| 🔒 internal | `source_table` | string |  |  |
| 🔒 internal | `tas_components` | string |  |  |
| 🔒 internal | `type` | string |  |  |
| 🔒 internal | `type_description` | string |  |  |
| 🔒 internal | `type_description_raw` | string |  |  |
| 🔒 internal | `type_raw` | string |  |  |
| 🔒 internal | `update_date` | timestamp[us] |  |  |
| 🔒 internal | `usaspending_snapshot_date` | date32[day] |  |  |
| 🔒 internal | `usaspending_unique_transaction_id` | string |  |  |