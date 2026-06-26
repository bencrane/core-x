"""Prompt-facing map decoders for edge_api — the field/op/enum/synonym subset the
TRANSLATE step renders into the ``emit_filter`` tool schema + cached system block.

Hand-mirrored from ``apps/catalyst_api/src/map_decoders.py`` (the AUTHORITATIVE
allowlist that EXECUTE enforces). The ``version`` MUST match catalyst's per dataset —
it is the cache-busting key for the translation memo, and a drift means the model is
prompted against a different allowlist than EXECUTE enforces (a hallucinated field
still gets rejected by EXECUTE, but the prompt should not invite it). The repo test
``tests/test_map_ask.py`` asserts the versions match.

This carries ONLY the prompt-facing data (field names, types, ops, enums, synonyms) —
never the Lance column names. Column resolution + the security allowlist live in
catalyst_api; this side only shapes the model's output space.

ROUTING: ``render_router_prompt`` / ``build_router_tool`` are the dataset-AUTO surface —
one forced-tool call that picks the dataset (awards | company | winners) AND compiles the
filters for it. ``ROUTER_VERSION`` + every per-dataset version key the auto memo, so any
axis change busts cached routings.
"""
from __future__ import annotations

OPS = ("=", ">=", "<=", "in", "between", "has", "has_any")

# Set-aside codes verified live against the awards serving table (18 distinct).
_SET_ASIDE_CODES = ("NONE", "SBA", "SBP", "8A", "8AN", "SDVOSBC", "SDVOSBS", "WOSB",
                    "WOSBSS", "EDWOSB", "EDWOSBSS", "HZC", "HZS", "ISBEE", "BI", "IEE",
                    "VSA", "VSS")

# GTM-attribute label axes on the awards serving table (awards.v9). MUST stay byte-identical to
# catalyst_api/src/map_decoders.py (the parity test asserts the enum value-sets are identical
# edge↔catalyst). NOTE the embedded COMMAS in "Facilities, Maintenance & Janitorial" and
# "Food, Agriculture & Beverage" — a wrong comma/& → zero rows on a Lance BITMAP scan.
_VERTICALS = (
    "Information Technology & Software", "Aerospace & Defense", "Construction",
    "Research & Development", "Professional & Management Services", "Healthcare & Life Sciences",
    "Facilities, Maintenance & Janitorial", "Engineering & Architecture", "Transportation & Logistics",
    "Food, Agriculture & Beverage", "Wholesale & Supply", "Environmental & Remediation",
    "Electronics & Instruments", "Telecommunications", "Energy & Utilities", "Industrial Manufacturing",
    "Financial & Insurance", "Security & Guard Services", "Government & Public Administration",
    "Education & Training", "Real Estate", "Media & Publishing", "Mining & Extraction",
    "Staffing & Human Capital")
_WORK_TYPES = ("services_labor", "manufacture", "distribute_resell", "construct", "RnD", "maintain_repair")
_EQUIP_INTENSITY = ("low", "medium", "high")

# PHASE-3 capability controlled vocabularies — MUST match catalyst_api/src/map_decoders.py
# (the parity test asserts enum value-sets are identical edge↔catalyst).
_CLEARANCE_LEVELS = ("PUBLIC_TRUST", "CONFIDENTIAL", "SECRET", "TOP_SECRET", "TS_SCI")
_CAPABILITY_TAGS = (
    "administrative_office_support", "aircraft_maintenance", "alarm_surveillance_systems",
    "architecture_services", "audio_visual_services", "behavioral_health_services",
    "calibration_inspection_qa", "chaplain_religious_services", "childcare_youth_services",
    "concrete_masonry", "construction_civil_heavy", "construction_general", "construction_vertical",
    "custodial_janitorial", "cybersecurity_services", "data_management_analytics", "demolition",
    "dental_services", "electrical_systems", "elevator_systems", "energy_renewables",
    "engineering_design", "environmental_remediation", "equipment_maintenance",
    "event_conference_support", "excavation_earthwork", "facilities_management", "fencing_barriers",
    "financial_audit_services", "fire_protection_systems", "flooring", "food_services", "fuel_supply",
    "grounds_maintenance_landscaping", "hvac_mechanical", "industrial_equipment_supply",
    "it_services", "laboratory_testing_services", "language_interpretation_translation",
    "laundry_linen_services", "legal_services", "lodging_billeting", "logistics_transportation",
    "mailroom_courier_services", "maintenance_repair_operations", "marine_vessel_services",
    "medical_clinical_services", "medical_equipment_supply", "moving_relocation", "nursing_services",
    "painting_coating", "paving_roadwork", "personnel_security_vetting", "pest_control",
    "physical_security_locksmith", "plumbing_pipefitting", "printing_publishing",
    "program_management_support", "public_affairs_communications", "renovation_alteration",
    "research_development", "roofing", "security_services_guard", "snow_ice_removal",
    "software_development", "staffing_personnel_services", "steel_structural", "supply_commodities",
    "surveying_mapping_gis", "telecom_networking", "training_instruction", "utilities_operation",
    "vehicle_fleet_maintenance", "veterinary_services", "warehousing_distribution",
    "waste_management", "water_wastewater")
_LABOR_CATEGORIES = (
    "carpenter", "crane_operator", "custodian", "dispatcher", "electrician", "equipment_operator",
    "food_service_worker", "general_laborer", "glazier", "heavy_equipment_operator",
    "hvac_technician", "instructor", "interpreter", "janitor", "licensed_practical_nurse",
    "locksmith", "mason", "medical_assistant", "millwright", "painter", "pest_control_technician",
    "pipefitter", "plumber", "program_manager", "project_manager", "quality_control_manager",
    "registered_nurse", "roofer", "safety_officer", "security_guard", "sheet_metal_worker",
    "site_superintendent", "surveyor", "translator", "truck_driver", "welder")

# Teaming prime canonical-name sets — MUST match catalyst_api/src/map_decoders.py (the synonym
# values are part of the version-keyed allowlist; the parity test asserts synonyms match).
_TEAMING_LOCKHEED = ("LOCKHEED MARTIN CORPORATION", "LOCKHEED MARTIN CORP")
_TEAMING_BOEING = ("BOEING COMPANY, THE", "THE BOEING COMPANY")
_TEAMING_RAYTHEON = ("RAYTHEON COMPANY",)
_TEAMING_NORTHROP = ("NORTHROP GRUMMAN SYSTEMS CORPORATION", "NORTHROP GRUMMAN SYSTEMS CORP")
_TEAMING_GENERAL_DYNAMICS = (
    "GENERAL DYNAMICS INFORMATION TECHNOLOGY, INC.", "GENERAL DYNAMICS MISSION SYSTEMS, INC.",
    "GENERAL DYNAMICS MISSION SYSTEMS, INC", "GENERAL DYNAMICS LAND SYSTEMS INC.",
    "GENERAL DYNAMICS GLOBAL FORCE, LLC", "GENERAL DYNAMICS ONE SOURCE LLC",
    "GENERAL DYNAMICS-OTS, INC.")
_TEAMING_LEIDOS = ("LEIDOS, INC.", "LEIDOS INC", "LEIDOS INNOVATIONS CORPORATION",
                   "LEIDOS BIOMEDICAL RESEARCH, INC.")
_TEAMING_BOOZ_ALLEN = ("BOOZ ALLEN HAMILTON INC.", "BOOZ ALLEN HAMILTON INC")
_TEAMING_SAIC = ("SCIENCE APPLICATIONS INTERNATIONAL CORPORATION",
                 "SCIENCE APPLICATIONS INTERNATIONAL CORP")

# Cert-token canonical sets for the SUB-only req_cert_tags axis — MUST match
# catalyst_api/src/map_decoders.py (the synonym values are part of the version-keyed allowlist;
# the parity test asserts the cert synonyms match byte-for-byte).
_CERT_CMMC = ("cmmc", "cmmc_l1", "cmmc_l2")
_CERT_ISO_9001 = ("iso_9001",)
_CERT_ISO_27001 = ("iso_27001",)
_CERT_ISO_14001 = ("iso_14001",)
_CERT_ISO_17025 = ("iso_17025",)
_CERT_AS9100 = ("as9100", "as9110")
_CERT_FAA_PART_145 = ("faa_part_145", "faa_part_145_certification", "faa_part_145_repair_station")
_CERT_CISSP = ("cissp",)
_CERT_PMP = ("pmp", "pmi_pmp", "project_management_professional")
_CERT_OSHA_30 = ("osha_30", "osha_30_hour")

DECODERS: dict[str, dict] = {
    "winners": {
        "version": "winners.v10",
        "description": "Federal-contract WINNERS — one row per entity that won a prime contract or a subaward. PRIME winners may also carry CAPABILITY signals extracted from their awards' solicitation documents (clearance, CMMC, what work they do, what trades they staff) — use these for 'companies that do X and require Y' questions. SUBAWARDEE winners additionally carry a TEAMING axis: which primes they have subcontracted under (last 5y), total teaming dollars, and how many distinct primes — use these for 'subs that have teamed with <prime>' and '$X+ teaming' questions. SUBAWARDEE winners also carry a SELF-REPORTED axis (the long tail of ~13.8k subs that assert their own capability/certifications, independent of any extracted solicitation scope): subaward_description_tag (what work the sub says it does) and req_cert_tag (certifications it holds, e.g. ISO 9001 / CMMC / AS9100) — use these for 'subs that self-report X' and 'subs with a <cert> certification' questions.",
        "fields": {
            "naics2":           {"type": "string", "ops": ("=", "in"), "desc": "2-digit NAICS sector ('23' = construction)"},
            "state":            {"type": "string", "ops": ("=", "in"), "desc": "2-letter US state of the winner"},
            "winner_type":      {"type": "string", "ops": ("=", "in"), "enum": ("prime_recipient", "subawardee")},
            "naics_code":       {"type": "string", "ops": ("=", "in"), "desc": "full NAICS code"},
            "total_obligation": {"type": "float",  "ops": (">=", "<=", "between"), "desc": "summed federal obligation (USD) per entity over the table's ~730-day (≈2y) coverage window — a rolled-up snapshot, not a lifetime total. For an entity total bounded to a SPECIFIC recent window, prefer the awards dataset aggregated by winner with days_since_action"},
            "award_count":      {"type": "int",    "ops": (">=", "<=", "between")},
            "days_since_last_award": {"type": "days_ago", "ops": ("<=", ">=", "between"),
                                      "desc": "whole days since the entity's most recent award action (integer; 0 = today). Time windows map here: 'won in the last N days' / 'past week' / 'this month' → days_since_last_award <= N"},
            # ── PHASE-3 capability axis (prime winners with extracted solicitation text) ──
            "has_extracted_scope":     {"type": "bool", "ops": ("=",), "desc": "true = the winner has ≥1 award with extracted solicitation scope (the ~1% slice the capability axes describe)"},
            "requires_clearance":      {"type": "bool", "ops": ("=",), "desc": "true = ≥1 covered award requires a personnel/facility security clearance"},
            "req_clearance_level_max": {"type": "string", "ops": ("=", "in"), "enum": _CLEARANCE_LEVELS,
                                        "desc": "highest clearance required across the winner's covered awards. 'secret clearance' → in [SECRET, TOP_SECRET, TS_SCI]"},
            "requires_cmmc":           {"type": "bool", "ops": ("=",), "desc": "true = ≥1 covered award requires CMMC certification"},
            "solicitation_scope_tag":          {"type": "list", "ops": ("has", "has_any"), "enum": _CAPABILITY_TAGS,
                                        "desc": "controlled capability the winner DOES (set membership). 'does electrical work' → solicitation_scope_tag has 'electrical_systems'. Use has (one tag) or has_any (list of tags)"},
            "labor_category":          {"type": "list", "ops": ("has", "has_any"), "enum": _LABOR_CATEGORIES,
                                        "desc": "skilled labor/trade the winner's covered awards staff. 'electricians' → labor_category has 'electrician'; 'cleared trades' → requires_clearance=true AND labor_category has_any the trade list"},
            # ── SUBAWARDEE teaming axis (which primes the sub has subcontracted under, last 5y) ──
            "teaming_dollars_5y":      {"type": "float", "ops": (">=", "<=", "between"),
                                        "desc": "total $ this SUBAWARDEE has subcontracted under prime contractors over the last 5 years. '$10M+ teaming' / 'over $10M in teaming' → teaming_dollars_5y >= 10000000. Subawardees only"},
            "n_teaming_primes":        {"type": "int", "ops": (">=", "<=", "between"),
                                        "desc": "count of DISTINCT primes this subawardee has teamed with (last 5y). 'teamed with 5+ primes' → n_teaming_primes >= 5. Subawardees only"},
            "teaming_prime":           {"type": "list", "ops": ("has", "has_any"),
                                        "desc": "EXACT full legal name(s) of primes the subawardee has subcontracted under. Use the known synonyms for major primes ('teamed with Lockheed' → use the 'lockheed' synonym); for a prime not in the synonyms, put it in unmapped (exact legal name required). Subawardees only"},
            # ── SUBAWARDEE self-reported axis (the long-tail signal subs assert about themselves) ──
            "subaward_description_tag": {"type": "list", "ops": ("has", "has_any"), "enum": _CAPABILITY_TAGS,
                                        "desc": "controlled capability the SUBAWARDEE self-reports doing (set membership; same 77-tag vocab as solicitation_scope_tag, but self-asserted across the FULL sub long tail rather than the scope-extracted slice). 'subs that self-report software development' → subaward_description_tag has 'software_development'. Subawardees only"},
            "req_cert_tag":            {"type": "list", "ops": ("has", "has_any"),
                                        "desc": "certification the SUBAWARDEE holds, as a granular open-vocabulary token (e.g. 'iso_9001', 'cmmc', 'as9100', 'cissp', 'pmp'). Prefer the known cert synonyms ('iso 9001', 'as9100', 'cmmc certification', 'faa part 145'); for a cert not in the synonyms put the exact lowercase token, or surface it in unmapped if unsure. Subawardees only"},
        },
        "synonyms": {
            "construction":  {"field": "naics2", "op": "=", "value": "23"},
            "subawardees":   {"field": "winner_type", "op": "=", "value": "subawardee"},
            "prime winners": {"field": "winner_type", "op": "=", "value": "prime_recipient"},
            "this week":     {"field": "days_since_last_award", "op": "<=", "value": 7},
            "won recently":  {"field": "days_since_last_award", "op": "<=", "value": 30},
            "cleared":          {"field": "requires_clearance", "op": "=", "value": True},
            "secret clearance": {"field": "req_clearance_level_max", "op": "in", "value": ["SECRET", "TOP_SECRET", "TS_SCI"]},
            "top secret":       {"field": "req_clearance_level_max", "op": "in", "value": ["TOP_SECRET", "TS_SCI"]},
            "cmmc":             {"field": "requires_cmmc", "op": "=", "value": True},
            "electrical":       {"field": "solicitation_scope_tag", "op": "has", "value": "electrical_systems"},
            "electricians":     {"field": "labor_category", "op": "has", "value": "electrician"},
            # SUBAWARDEE teaming phrasings (prime names are exact stored legal names → has_any).
            "lockheed":          {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_LOCKHEED)},
            "lockheed martin":   {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_LOCKHEED)},
            "boeing":            {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_BOEING)},
            "raytheon":          {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_RAYTHEON)},
            "rtx":               {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_RAYTHEON)},
            "northrop":          {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_NORTHROP)},
            "northrop grumman":  {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_NORTHROP)},
            "general dynamics":  {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_GENERAL_DYNAMICS)},
            "leidos":            {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_LEIDOS)},
            "booz allen":        {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_BOOZ_ALLEN)},
            "saic":              {"field": "teaming_prime", "op": "has_any", "value": list(_TEAMING_SAIC)},
            # SUBAWARDEE self-reported phrasings (route the self-report cue to the UNGATED axis).
            "self-report software development":   {"field": "subaward_description_tag", "op": "has", "value": "software_development"},
            "self-reported software development": {"field": "subaward_description_tag", "op": "has", "value": "software_development"},
            "self-report aircraft maintenance":   {"field": "subaward_description_tag", "op": "has", "value": "aircraft_maintenance"},
            "self-reported aircraft maintenance": {"field": "subaward_description_tag", "op": "has", "value": "aircraft_maintenance"},
            # Certification phrasings → has_any over the EXACT stored cert tokens (open vocab).
            "cmmc certification":  {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_CMMC)},
            "iso 9001":            {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_ISO_9001)},
            "iso 27001":           {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_ISO_27001)},
            "iso 14001":           {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_ISO_14001)},
            "iso 17025":           {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_ISO_17025)},
            "as9100":              {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_AS9100)},
            "faa part 145":        {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_FAA_PART_145)},
            "cissp":               {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_CISSP)},
            "pmp":                 {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_PMP)},
            "osha 30":             {"field": "req_cert_tag", "op": "has_any", "value": list(_CERT_OSHA_30)},
        },
        "aggregate": {
            "measure": "entity_obligated_usd",
            "dims": ["naics2", "naics_code", "state", "winner_type"],
            "pseudo_dims": ["winner", "size_band"],
            "metrics": ["count", "sum", "avg", "median", "p90"],
            "desc": ("For BREAKDOWN / TOTAL / DISTRIBUTION / RANKING over winners ('total won by"
                     " state', 'top winners', 'winners by NAICS', 'distribution of winner sizes'),"
                     " ALSO emit an aggregate object {group_by, metrics?, limit?}. group_by is a"
                     " dim below, or 'winner' (top entities by $) or 'size_band' (obligation"
                     " histogram). Omit aggregate for plain 'show me' rows."),
        },
    },
    "company": {
        "version": "company.v5",
        "description": "Companies in the firmographics target universe that are SAM-registered — one row per company.",
        "fields": {
            "naics2":             {"type": "string", "ops": ("=", "in"), "desc": "2-digit NAICS sector ('23' = construction)"},
            "industry":           {"type": "string", "ops": ("=", "in"), "desc": "LinkedIn-style industry label"},
            "employee_size_band": {"type": "string", "ops": ("=", "in"), "enum": ("1-10", "11-50", "51-200", "201-500", "501-1000", "1001-5000", "5001-10000", "10001+"), "desc": "headcount band, e.g. '11-50', '51-200'"},
            "company_type":       {"type": "string", "ops": ("=", "in"), "enum": ("Educational", "Educational Institution", "Government Agency", "Nonprofit", "Partnership", "Privately Held", "Public Company", "Self-Employed", "Self-Owned", "Sole Proprietorship")},
            "state":              {"type": "string", "ops": ("=", "in"), "desc": "2-letter US state (physical address)"},
            "has_federal_awards": {"type": "bool",   "ops": ("=",), "desc": "true = the company holds federal awards"},
            "is_active":          {"type": "bool",   "ops": ("=",), "desc": "true = active SAM registration"},
            "primary_naics":      {"type": "string", "ops": ("=", "in")},
            "founded_year":       {"type": "int",    "ops": (">=", "<=", "between")},
            "active_obligations": {"type": "float",  "ops": (">=", "<=", "between"), "desc": "total active federal obligations, USD"},
            "award_count":        {"type": "int",    "ops": (">=", "<=", "between")},
            "days_since_last_award": {"type": "days_ago", "ops": ("<=", ">=", "between"),
                                      "desc": "whole days since the company's most recent federal award action (integer; 0 = today). Time windows map here: 'won in the last N days' / 'past week' / 'this month' → days_since_last_award <= N"},
        },
        "synonyms": {
            "construction":        {"field": "naics2", "op": "=", "value": "23"},
            "federal contractors": {"field": "has_federal_awards", "op": "=", "value": True},
            "active":              {"field": "is_active", "op": "=", "value": True},
            "this week":           {"field": "days_since_last_award", "op": "<=", "value": 7},
            "won recently":        {"field": "days_since_last_award", "op": "<=", "value": 30},
        },
        "aggregate": {
            "measure": "entity_active_obligated_usd",
            "dims": ["naics2", "industry", "employee_size_band", "company_type", "state", "primary_naics"],
            "pseudo_dims": ["winner", "size_band"],
            "metrics": ["count", "sum", "avg", "median", "p90"],
            "desc": ("For BREAKDOWN / TOTAL / DISTRIBUTION / RANKING over companies ('total active"
                     " obligations by industry', 'companies by employee size', 'top companies by"
                     " active $', 'distribution by company size'), ALSO emit an aggregate object"
                     " {group_by, metrics?, limit?}. group_by is a firmographic dim below, or"
                     " 'winner' (top companies by $) or 'size_band' (obligation histogram). Omit"
                     " aggregate for plain 'show me' rows."),
        },
    },
    "awards": {
        "version": "awards.v10",
        "description": "Individual federal AWARD ACTIONS — one row per positive-dollar PRIME contract action. THE table for 'won an award over $X in the last N days': the amount is the single action's dollars, never a lifetime or window rollup.",
        "fields": {
            "award_amount":      {"type": "float", "ops": (">=", "<=", "between"), "desc": "the single award action's dollars, USD ('won an award over $1M' → award_amount >= 1000000)"},
            "days_since_action": {"type": "days_ago", "ops": ("<=", ">=", "between"),
                                  "desc": "whole days since the award action (integer; 0 = today). 'won in the last N days' / 'this week' → days_since_action <= N"},
            "fiscal_year":       {"type": "int", "ops": ("=", "in"), "desc": "US federal fiscal year of the action (Oct–Sep; FY2025 = Oct 2024–Sep 2025). EXPLICIT year only: 'FY2025' / 'fiscal year 2025' → fiscal_year = 2025; 'FY24 and FY25' → fiscal_year in [2024, 2025]. For a RELATIVE window ('in the last N days') use days_since_action, not fiscal_year. Group-by fiscal_year for a year-over-year spend trend"},
            "naics2":            {"type": "string", "ops": ("=", "in"), "desc": "2-digit NAICS sector ('23' = construction) — the vendor's INDUSTRY"},
            "naics_code":        {"type": "string", "ops": ("=", "in"), "desc": "full NAICS code"},
            "psc_category":      {"type": "string", "ops": ("=", "in"), "desc": "leading char of the Product/Service Code = WHAT THE CONTRACT BUYS (distinct from NAICS, the vendor's industry). 'V' = Transportation/Travel/Relocation services. Use for 'transportation/freight SERVICES' / 'PSC category V' — catches freight & logistics service contracts coded under a non-48/49 NAICS. Prime awards only"},
            "psc_code":          {"type": "string", "ops": ("=", "in"), "desc": "full Product/Service Code, UPPERCASE (e.g. 'V111' = motor freight, 'V112' = air freight). Prime awards only"},
            "state":             {"type": "string", "ops": ("=", "in"), "desc": "winner's HQ/registration state, 2-letter — use for 'companies in <state>'"},
            "city":              {"type": "string", "ops": ("=", "in"), "desc": "winner's HQ city, UPPERCASE (e.g. 'DALLAS') — use for 'companies in <city>'"},
            "county":            {"type": "string", "ops": ("=", "in"), "desc": "winner's HQ county name, UPPERCASE without the word 'County' (e.g. 'TARRANT'); prime awards only"},
            "pop_state":         {"type": "string", "ops": ("=", "in"), "desc": "2-letter state where the WORK IS PERFORMED — use for 'performing work in / projects in <state>'"},
            "pop_city":          {"type": "string", "ops": ("=", "in"), "desc": "UPPERCASE city where the WORK IS PERFORMED — use for 'projects in <city>'"},
            "awarding_agency":   {"type": "string", "ops": ("=", "in"), "desc": "EXACT top-tier awarding department name, e.g. 'Department of Defense', 'Department of Veterans Affairs', 'General Services Administration', 'Department of Homeland Security'"},
            "awarding_sub_agency": {"type": "string", "ops": ("=", "in"), "desc": "EXACT sub-tier awarding agency name, e.g. 'Federal Acquisition Service', 'Federal Aviation Administration', 'U.S. Coast Guard', 'National Institutes of Health'"},
            "set_aside":         {"type": "string", "ops": ("=", "in"), "enum": _SET_ASIDE_CODES,
                                  "desc": "set-aside code: 8A/8AN = 8(a); SDVOSBC/SDVOSBS = service-disabled-veteran-owned; WOSB/WOSBSS/EDWOSB/EDWOSBSS = woman-owned; HZC/HZS = HUBZone; SBA/SBP = small-business; 'NONE' = explicitly no set-aside. Partial coverage: many actions carry no signal"},
            "business_size":     {"type": "string", "ops": ("=", "in"), "enum": ("SMALL BUSINESS", "OTHER THAN SMALL BUSINESS"), "desc": "contracting officer's business-size determination. 'small business' → 'SMALL BUSINESS'. Prime actions only"},
            "winner_type":       {"type": "string", "ops": ("=", "in"), "enum": ("prime_recipient", "subawardee")},
            "action_type":       {"type": "string", "ops": ("=", "in"), "desc": "FPDS action type of this action, e.g. 'EXERCISE AN OPTION', 'FUNDING ONLY ACTION', 'CHANGE ORDER', 'SUPPLEMENTAL AGREEMENT FOR WORK WITHIN SCOPE'. Group-by action_type for the action mix. Prefer is_option_exercise for the option-exercise case"},
            "is_option_exercise": {"type": "bool", "ops": ("=",), "desc": "true = this action EXERCISED A CONTRACT OPTION — the government committing the next work tranche on an existing contract (a mobilization-capital trigger; the entity is already invested). 'option exercise' / 'exercised option' / 'mobilization' → is_option_exercise = true; award_amount is then the mobilization $"},
            "is_active":         {"type": "bool", "ops": ("=",), "desc": "true = the contract is currently within its period of performance (active). Prime actions only — subawards carry no PoP and are excluded. 'active contracts' / 'currently active' → is_active = true"},
            # ── GTM label axes (awards.v9): the award's (NAICS, PSC) pair → a rich VERTICAL, a
            # WORK_TYPE (make vs resell vs build), and an EQUIPMENT_INTENSITY band. All three live
            # as BITMAP columns; head-coverage (~80% of both-codes $) so unlabeled rows surface in
            # 'not applied', never silently filtered. award grain. (what_was_done is a DISPLAY-only
            # gloss surfaced in notes — NOT a filter field.) ──
            "vertical":          {"type": "string", "ops": ("=", "in"), "enum": _VERTICALS,
                                  "desc": "the award's INDUSTRY VERTICAL — a 24-name GTM taxonomy derived from the (NAICS, PSC) pair (distinct from naics2, the raw 2-digit sector). 'IT companies' / 'the aerospace vertical' / 'healthcare sector' → vertical = the matching name. Partial coverage: only the labeled head (~80% of both-codes $); unlabeled rows carry no signal (surface as not-applied)"},
            "work_type":         {"type": "string", "ops": ("=", "in"), "enum": _WORK_TYPES,
                                  "desc": "WHAT THE VENDOR DOES on the award (the make-vs-resell axis). services_labor = labor/services; manufacture = makes the goods; distribute_resell = resells/distributes; construct = construction work; RnD = research & development; maintain_repair = maintenance/repair. 'manufacturers'/'makers' → 'manufacture'; 'resellers'/'distributors' → 'distribute_resell'"},
            "equipment_intensity": {"type": "string", "ops": ("=", "in"), "enum": _EQUIP_INTENSITY,
                                  "desc": "how EQUIPMENT-HEAVY the work is (a mobilization-capital / financing signal). 'equipment-heavy'/'capital-intensive' → 'high'; 'asset-light'/'labor-only' → 'low'"},
        },
        "synonyms": {
            "construction":   {"field": "naics2", "op": "=", "value": "23"},
            "active":         {"field": "is_active", "op": "=", "value": True},
            "active contracts": {"field": "is_active", "op": "=", "value": True},
            "this week":      {"field": "days_since_action", "op": "<=", "value": 7},
            "this month":     {"field": "days_since_action", "op": "<=", "value": 30},
            "won recently":   {"field": "days_since_action", "op": "<=", "value": 30},
            "subawards":      {"field": "winner_type", "op": "=", "value": "subawardee"},
            "dod":            {"field": "awarding_agency", "op": "in",
                               "value": ["Department of Defense", "Department of Defense (DOD)"]},
            "gsa":            {"field": "awarding_agency", "op": "=", "value": "General Services Administration"},
            "the va":         {"field": "awarding_agency", "op": "=", "value": "Department of Veterans Affairs"},
            "8(a)":           {"field": "set_aside", "op": "in", "value": ["8A", "8AN"]},
            "sdvosb":         {"field": "set_aside", "op": "in", "value": ["SDVOSBC", "SDVOSBS"]},
            "hubzone":        {"field": "set_aside", "op": "in", "value": ["HZC", "HZS"]},
            "woman-owned":    {"field": "set_aside", "op": "in",
                               "value": ["WOSB", "WOSBSS", "EDWOSB", "EDWOSBSS"]},
            "transportation services": {"field": "psc_category", "op": "=", "value": "V"},
            "freight services":        {"field": "psc_category", "op": "=", "value": "V"},
            "psc v":                   {"field": "psc_category", "op": "=", "value": "V"},
            "small business":          {"field": "business_size", "op": "=", "value": "SMALL BUSINESS"},
            "option exercise":         {"field": "is_option_exercise", "op": "=", "value": True},
            "option exercises":        {"field": "is_option_exercise", "op": "=", "value": True},
            "exercised option":        {"field": "is_option_exercise", "op": "=", "value": True},
            "options exercised":       {"field": "is_option_exercise", "op": "=", "value": True},
            # ── GTM label-axis lexicon (awards.v9). Plain-English → vertical / work_type /
            # equipment_intensity. Byte-identical to the catalyst mirror. Head-coverage only;
            # bare "construction" stays on naics2 above (broad recall). Ambiguous bare tokens
            # (security, engineering, manufacturing, logistics, defense) deliberately NOT mapped. ──
            "it contracts":            {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "software contracts":      {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "software vendors":        {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "cybersecurity":           {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "cyber contracts":         {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "information technology":  {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "aerospace":               {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "aerospace contracts":     {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "defense contractors":     {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "aerospace and defense":   {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "weapons systems":         {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "construction industry":   {"field": "vertical", "op": "=", "value": "Construction"},
            "building contractors":    {"field": "vertical", "op": "=", "value": "Construction"},
            "general contractors":     {"field": "vertical", "op": "=", "value": "Construction"},
            "r&d vertical":            {"field": "vertical", "op": "=", "value": "Research & Development"},
            "research and development": {"field": "vertical", "op": "=", "value": "Research & Development"},
            "research labs":           {"field": "vertical", "op": "=", "value": "Research & Development"},
            "professional services":   {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "management consulting":   {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "management services":     {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "consulting firms":        {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "healthcare":              {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "healthcare contracts":    {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "medical services":        {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "life sciences":           {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "pharma":                  {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "facilities management":   {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "janitorial":              {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "custodial services":      {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "facilities maintenance":  {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "engineering and architecture": {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "architecture firms":      {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "a&e firms":               {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "architectural services":  {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "transportation and logistics": {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "logistics contractors":   {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "trucking":                {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "freight carriers":        {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "food and agriculture":    {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "food services vertical":  {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "agriculture":             {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "food and beverage":       {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "wholesale and supply":    {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "supply contractors":      {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "wholesalers":             {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "commodity suppliers":     {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "environmental":           {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "environmental remediation": {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "remediation contractors": {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "environmental cleanup":   {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "electronics":             {"field": "vertical", "op": "=", "value": "Electronics & Instruments"},
            "electronics and instruments": {"field": "vertical", "op": "=", "value": "Electronics & Instruments"},
            "instrumentation":         {"field": "vertical", "op": "=", "value": "Electronics & Instruments"},
            "telecom":                 {"field": "vertical", "op": "=", "value": "Telecommunications"},
            "telecommunications":      {"field": "vertical", "op": "=", "value": "Telecommunications"},
            "telecom contractors":     {"field": "vertical", "op": "=", "value": "Telecommunications"},
            "energy and utilities":    {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "utilities":               {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "power and energy":        {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "energy contractors":      {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "industrial manufacturing": {"field": "vertical", "op": "=", "value": "Industrial Manufacturing"},
            "manufacturing vertical":  {"field": "vertical", "op": "=", "value": "Industrial Manufacturing"},
            "industrial manufacturers": {"field": "vertical", "op": "=", "value": "Industrial Manufacturing"},
            "financial services":      {"field": "vertical", "op": "=", "value": "Financial & Insurance"},
            "financial and insurance": {"field": "vertical", "op": "=", "value": "Financial & Insurance"},
            "insurance contractors":   {"field": "vertical", "op": "=", "value": "Financial & Insurance"},
            "guard services":          {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "security guards":         {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "physical security":       {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "armed guard services":    {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "public administration":   {"field": "vertical", "op": "=", "value": "Government & Public Administration"},
            "government administration": {"field": "vertical", "op": "=", "value": "Government & Public Administration"},
            "education and training":  {"field": "vertical", "op": "=", "value": "Education & Training"},
            "training contractors":    {"field": "vertical", "op": "=", "value": "Education & Training"},
            "educational services":    {"field": "vertical", "op": "=", "value": "Education & Training"},
            "real estate":             {"field": "vertical", "op": "=", "value": "Real Estate"},
            "real estate services":    {"field": "vertical", "op": "=", "value": "Real Estate"},
            "leasing services":        {"field": "vertical", "op": "=", "value": "Real Estate"},
            "media and publishing":    {"field": "vertical", "op": "=", "value": "Media & Publishing"},
            "publishing":              {"field": "vertical", "op": "=", "value": "Media & Publishing"},
            "media services":          {"field": "vertical", "op": "=", "value": "Media & Publishing"},
            "mining":                  {"field": "vertical", "op": "=", "value": "Mining & Extraction"},
            "mining and extraction":   {"field": "vertical", "op": "=", "value": "Mining & Extraction"},
            "extraction contractors":  {"field": "vertical", "op": "=", "value": "Mining & Extraction"},
            "aec":                     {"field": "vertical", "op": "in",
                                        "value": ["Engineering & Architecture", "Construction"]},
            "manufacturers":           {"field": "work_type", "op": "=", "value": "manufacture"},
            "makers":                  {"field": "work_type", "op": "=", "value": "manufacture"},
            "product manufacturers":   {"field": "work_type", "op": "=", "value": "manufacture"},
            "resellers":               {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "distributors":            {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "value-added resellers":   {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "vars":                    {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "warehousing":             {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "maintenance and repair":  {"field": "work_type", "op": "=", "value": "maintain_repair"},
            "repair services":         {"field": "work_type", "op": "=", "value": "maintain_repair"},
            "services firms":          {"field": "work_type", "op": "=", "value": "services_labor"},
            "labor services":          {"field": "work_type", "op": "=", "value": "services_labor"},
            "r&d work":                {"field": "work_type", "op": "=", "value": "RnD"},
            "research work":           {"field": "work_type", "op": "=", "value": "RnD"},
            "equipment-heavy":         {"field": "equipment_intensity", "op": "=", "value": "high"},
            "equipment-intensive":     {"field": "equipment_intensity", "op": "=", "value": "high"},
            "capital-intensive":       {"field": "equipment_intensity", "op": "=", "value": "high"},
            "asset-heavy":             {"field": "equipment_intensity", "op": "=", "value": "high"},
            "asset-light":             {"field": "equipment_intensity", "op": "=", "value": "low"},
            "labor-only":              {"field": "equipment_intensity", "op": "=", "value": "low"},
        },
        # Dataset-specific prompt rules (rendered under Rules).
        "notes": (
            "Geo disambiguation: 'companies/winners in <place>' → state/city/county (the"
            " winner's HQ). 'performing work in / projects in / work located in <place>' →"
            " pop_state/pop_city.",
            "NAICS vs PSC: naics2/naics_code = the vendor's INDUSTRY; psc_category/psc_code ="
            " WHAT THE CONTRACT BUYS. For 'transportation/freight SERVICES' or 'PSC category V'"
            " use psc_category='V' (not a NAICS filter) — many transport/logistics service"
            " awards carry a non-48/49 NAICS, so a NAICS-only filter misses them.",
            "Industry axes: naics2/naics_code = the RAW federal sector code; vertical = the 24-name"
            " GTM taxonomy over the (NAICS, PSC) pair. 'industry sector code' or a literal 2-digit"
            " code → naics2; 'the <name> vertical/sector/industry' → vertical. work_type (make vs"
            " resell vs construct vs services vs RnD vs maintain_repair) and equipment_intensity"
            " (low/medium/high financing signal) are PEER filters that AND with naics2/psc. All"
            " three have partial coverage — unlabeled rows surface in 'not applied', never silently"
            " filtered. what_was_done is a free-text DISPLAY field (shown for context, not filterable).",
        ),
        # Aggregate capability (mirrors catalyst_api AggregateSpec; the parity test asserts
        # dims+metrics match). Prompt-facing only — no Lance column names.
        "aggregate": {
            "measure": "action_obligated_usd",
            "dims": ["fiscal_year", "naics2", "naics_code", "psc_category", "psc_code", "awarding_agency",
                     "awarding_sub_agency", "state", "pop_state", "set_aside", "business_size",
                     "action_type", "winner_type",
                     "vertical", "work_type", "equipment_intensity"],
            "pseudo_dims": ["winner", "size_band"],
            "metrics": ["count", "sum", "avg", "median", "p90"],
            "desc": ("For BREAKDOWN / TOTAL / DISTRIBUTION / RANKING questions ('break down by"
                     " X', 'total $ of', 'how much', 'by agency/state/PSC', 'top N winners',"
                     " 'distribution of award sizes / by size'), ALSO emit an aggregate object"
                     " {group_by, metrics?, limit?}. group_by is a dim below, or 'winner' (top"
                     " entities by $) or 'size_band' (award-amount histogram). The time window"
                     " STILL rides in filters via days_since_action — the aggregate respects it;"
                     " never assume a window. Omit aggregate entirely for plain 'show me' rows."),
        },
    },
    "active": {
        "version": "active.v3",
        "description": "ACTIVE prime awards still in performance — one row per award (award grain). The FORWARD-looking table: it carries the period-of-performance end date, so you can find incumbents about to RECOMPETE (contracts whose performance ends in the next N days). Use for 'expiring / up for recompete / runway' questions; amounts are the award's current/potential value, not a single action.",
        "fields": {
            "days_until_expiry": {"type": "days_ahead", "ops": ("<=", ">=", "between"), "desc": "whole days until the award's period of performance ends. 'expiring in the next N days' / 'recompete within N days' → days_until_expiry <= N; 'at least N days of runway' → days_until_expiry >= N. THE recompete axis"},
            "current_value": {"type": "float", "ops": (">=", "<=", "between"), "desc": "current total value of the award, USD ('contracts over $1M' → current_value >= 1000000)"},
            "potential_value": {"type": "float", "ops": (">=", "<=", "between"), "desc": "potential total value incl. unexercised options, USD"},
            "obligated": {"type": "float", "ops": (">=", "<=", "between"), "desc": "total dollars obligated to date, USD"},
            "naics2": {"type": "string", "ops": ("=", "in"), "desc": "2-digit NAICS sector ('23' = construction) — the vendor's INDUSTRY"},
            "naics_code": {"type": "string", "ops": ("=", "in"), "desc": "full NAICS code"},
            # ── GTM label axes (active.v2): the award's (NAICS, PSC) pair → a rich VERTICAL, a
            # WORK_TYPE (make vs resell vs build), and an EQUIPMENT_INTENSITY band. All three live
            # as BITMAP columns; head-coverage (~78% of recompete $, ~38% of rows) so unlabeled rows
            # surface in 'not applied', never silently filtered. award grain. (what_was_done is a
            # DISPLAY-only gloss surfaced in notes — NOT a filter field.) ──
            "vertical":          {"type": "string", "ops": ("=", "in"), "enum": _VERTICALS,
                                  "desc": "the award's INDUSTRY VERTICAL — a 24-name GTM taxonomy derived from the (NAICS, PSC) pair (distinct from naics2, the raw 2-digit sector). 'the IT vertical' / 'aerospace contracts' / 'healthcare sector' (up for recompete) → vertical = the matching name. Partial coverage: the labeled head is ~78% of recompete $ (the unlabeled tail is small-dollar — ~38% of rows); unlabeled rows carry no signal (surface as not-applied)"},
            "work_type":         {"type": "string", "ops": ("=", "in"), "enum": _WORK_TYPES,
                                  "desc": "WHAT THE VENDOR DOES on the award (the make-vs-resell axis). services_labor = labor/services; manufacture = makes the goods; distribute_resell = resells/distributes; construct = construction work; RnD = research & development; maintain_repair = maintenance/repair. 'manufacturers'/'makers' → 'manufacture'; 'resellers'/'distributors' → 'distribute_resell'"},
            "equipment_intensity": {"type": "string", "ops": ("=", "in"), "enum": _EQUIP_INTENSITY,
                                  "desc": "how EQUIPMENT-HEAVY the work is (a mobilization-capital / financing signal). 'equipment-heavy'/'capital-intensive' → 'high'; 'asset-light'/'labor-only' → 'low'"},
            "psc_category": {"type": "string", "ops": ("=", "in"), "desc": "leading PSC char = WHAT THE CONTRACT BUYS. 'V' = Transportation/Travel/Relocation services (catches transport/freight services coded under a non-48/49 NAICS)"},
            "psc_code": {"type": "string", "ops": ("=", "in"), "desc": "full Product/Service Code, UPPERCASE (e.g. 'V111')"},
            "state": {"type": "string", "ops": ("=", "in"), "desc": "winner HQ state, 2-letter — 'companies in <state>'"},
            "pop_state": {"type": "string", "ops": ("=", "in"), "desc": "2-letter state where the WORK IS PERFORMED — 'work in <state>'"},
            "awarding_agency": {"type": "string", "ops": ("=", "in"), "desc": "EXACT top-tier awarding department name, e.g. 'Department of Defense', 'Department of Homeland Security'"},
            "awarding_sub_agency": {"type": "string", "ops": ("=", "in"), "desc": "EXACT sub-tier awarding agency name"},
            "set_aside": {"type": "string", "ops": ("=", "in"), "enum": _SET_ASIDE_CODES, "desc": "set-aside code: 8A/8AN=8(a); SDVOSBC/SDVOSBS=service-disabled-veteran-owned; WOSB/WOSBSS/EDWOSB/EDWOSBSS=woman-owned; HZC/HZS=HUBZone; 'NONE'=no set-aside"},
            "business_size": {"type": "string", "ops": ("=", "in"), "enum": ("SMALL BUSINESS", "OTHER THAN SMALL BUSINESS"), "desc": "contracting officer's business-size determination. 'small business' → 'SMALL BUSINESS'"},
            "has_option_tail": {"type": "bool", "ops": ("=",), "desc": "true = the govt holds an unexercised option beyond the current end (extra runway before a hard recompete)"},
            "award_or_idv_flag": {"type": "string", "ops": ("=", "in"), "enum": ("AWARD", "IDV"), "desc": "'AWARD' = a definitive contract; 'IDV' = an indefinite-delivery vehicle"},
        },
        "synonyms": {
            "construction": {"field": "naics2", "op": "=", "value": "23"},
            "transportation services": {"field": "psc_category", "op": "=", "value": "V"},
            "freight services": {"field": "psc_category", "op": "=", "value": "V"},
            "expiring soon": {"field": "days_until_expiry", "op": "<=", "value": 90},
            "expiring this quarter": {"field": "days_until_expiry", "op": "<=", "value": 90},
            "recompete": {"field": "days_until_expiry", "op": "<=", "value": 180},
            "up for recompete": {"field": "days_until_expiry", "op": "<=", "value": 180},
            "small business": {"field": "business_size", "op": "=", "value": "SMALL BUSINESS"},
            "8(a)": {"field": "set_aside", "op": "in", "value": ["8A", "8AN"]},
            "sdvosb": {"field": "set_aside", "op": "in", "value": ["SDVOSBC", "SDVOSBS"]},
            "hubzone": {"field": "set_aside", "op": "in", "value": ["HZC", "HZS"]},
            "woman-owned": {"field": "set_aside", "op": "in", "value": ["WOSB", "WOSBSS", "EDWOSB", "EDWOSBSS"]},
            "dod": {"field": "awarding_agency", "op": "in", "value": ["Department of Defense", "Department of Defense (DOD)"]},
            # ── GTM label-axis lexicon (active.v2). Plain-English → vertical / work_type /
            # equipment_intensity. Byte-identical to the catalyst mirror + the awards decoder.
            # Head-coverage only; bare "construction" stays on naics2 above (broad recall). Ambiguous
            # bare tokens (security, engineering, manufacturing, logistics, defense) deliberately NOT mapped. ──
            "it contracts":            {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "software contracts":      {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "software vendors":        {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "cybersecurity":           {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "cyber contracts":         {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "information technology":  {"field": "vertical", "op": "=", "value": "Information Technology & Software"},
            "aerospace":               {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "aerospace contracts":     {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "defense contractors":     {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "aerospace and defense":   {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "weapons systems":         {"field": "vertical", "op": "=", "value": "Aerospace & Defense"},
            "construction industry":   {"field": "vertical", "op": "=", "value": "Construction"},
            "building contractors":    {"field": "vertical", "op": "=", "value": "Construction"},
            "general contractors":     {"field": "vertical", "op": "=", "value": "Construction"},
            "r&d vertical":            {"field": "vertical", "op": "=", "value": "Research & Development"},
            "research and development": {"field": "vertical", "op": "=", "value": "Research & Development"},
            "research labs":           {"field": "vertical", "op": "=", "value": "Research & Development"},
            "professional services":   {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "management consulting":   {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "management services":     {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "consulting firms":        {"field": "vertical", "op": "=", "value": "Professional & Management Services"},
            "healthcare":              {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "healthcare contracts":    {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "medical services":        {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "life sciences":           {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "pharma":                  {"field": "vertical", "op": "=", "value": "Healthcare & Life Sciences"},
            "facilities management":   {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "janitorial":              {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "custodial services":      {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "facilities maintenance":  {"field": "vertical", "op": "=", "value": "Facilities, Maintenance & Janitorial"},
            "engineering and architecture": {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "architecture firms":      {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "a&e firms":               {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "architectural services":  {"field": "vertical", "op": "=", "value": "Engineering & Architecture"},
            "transportation and logistics": {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "logistics contractors":   {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "trucking":                {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "freight carriers":        {"field": "vertical", "op": "=", "value": "Transportation & Logistics"},
            "food and agriculture":    {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "food services vertical":  {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "agriculture":             {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "food and beverage":       {"field": "vertical", "op": "=", "value": "Food, Agriculture & Beverage"},
            "wholesale and supply":    {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "supply contractors":      {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "wholesalers":             {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "commodity suppliers":     {"field": "vertical", "op": "=", "value": "Wholesale & Supply"},
            "environmental":           {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "environmental remediation": {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "remediation contractors": {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "environmental cleanup":   {"field": "vertical", "op": "=", "value": "Environmental & Remediation"},
            "electronics":             {"field": "vertical", "op": "=", "value": "Electronics & Instruments"},
            "electronics and instruments": {"field": "vertical", "op": "=", "value": "Electronics & Instruments"},
            "instrumentation":         {"field": "vertical", "op": "=", "value": "Electronics & Instruments"},
            "telecom":                 {"field": "vertical", "op": "=", "value": "Telecommunications"},
            "telecommunications":      {"field": "vertical", "op": "=", "value": "Telecommunications"},
            "telecom contractors":     {"field": "vertical", "op": "=", "value": "Telecommunications"},
            "energy and utilities":    {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "utilities":               {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "power and energy":        {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "energy contractors":      {"field": "vertical", "op": "=", "value": "Energy & Utilities"},
            "industrial manufacturing": {"field": "vertical", "op": "=", "value": "Industrial Manufacturing"},
            "manufacturing vertical":  {"field": "vertical", "op": "=", "value": "Industrial Manufacturing"},
            "industrial manufacturers": {"field": "vertical", "op": "=", "value": "Industrial Manufacturing"},
            "financial services":      {"field": "vertical", "op": "=", "value": "Financial & Insurance"},
            "financial and insurance": {"field": "vertical", "op": "=", "value": "Financial & Insurance"},
            "insurance contractors":   {"field": "vertical", "op": "=", "value": "Financial & Insurance"},
            "guard services":          {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "security guards":         {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "physical security":       {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "armed guard services":    {"field": "vertical", "op": "=", "value": "Security & Guard Services"},
            "public administration":   {"field": "vertical", "op": "=", "value": "Government & Public Administration"},
            "government administration": {"field": "vertical", "op": "=", "value": "Government & Public Administration"},
            "education and training":  {"field": "vertical", "op": "=", "value": "Education & Training"},
            "training contractors":    {"field": "vertical", "op": "=", "value": "Education & Training"},
            "educational services":    {"field": "vertical", "op": "=", "value": "Education & Training"},
            "real estate":             {"field": "vertical", "op": "=", "value": "Real Estate"},
            "real estate services":    {"field": "vertical", "op": "=", "value": "Real Estate"},
            "leasing services":        {"field": "vertical", "op": "=", "value": "Real Estate"},
            "media and publishing":    {"field": "vertical", "op": "=", "value": "Media & Publishing"},
            "publishing":              {"field": "vertical", "op": "=", "value": "Media & Publishing"},
            "media services":          {"field": "vertical", "op": "=", "value": "Media & Publishing"},
            "mining":                  {"field": "vertical", "op": "=", "value": "Mining & Extraction"},
            "mining and extraction":   {"field": "vertical", "op": "=", "value": "Mining & Extraction"},
            "extraction contractors":  {"field": "vertical", "op": "=", "value": "Mining & Extraction"},
            "aec":                     {"field": "vertical", "op": "in",
                                        "value": ["Engineering & Architecture", "Construction"]},
            "manufacturers":           {"field": "work_type", "op": "=", "value": "manufacture"},
            "makers":                  {"field": "work_type", "op": "=", "value": "manufacture"},
            "product manufacturers":   {"field": "work_type", "op": "=", "value": "manufacture"},
            "resellers":               {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "distributors":            {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "value-added resellers":   {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "vars":                    {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "warehousing":             {"field": "work_type", "op": "=", "value": "distribute_resell"},
            "maintenance and repair":  {"field": "work_type", "op": "=", "value": "maintain_repair"},
            "repair services":         {"field": "work_type", "op": "=", "value": "maintain_repair"},
            "services firms":          {"field": "work_type", "op": "=", "value": "services_labor"},
            "labor services":          {"field": "work_type", "op": "=", "value": "services_labor"},
            "r&d work":                {"field": "work_type", "op": "=", "value": "RnD"},
            "research work":           {"field": "work_type", "op": "=", "value": "RnD"},
            "equipment-heavy":         {"field": "equipment_intensity", "op": "=", "value": "high"},
            "equipment-intensive":     {"field": "equipment_intensity", "op": "=", "value": "high"},
            "capital-intensive":       {"field": "equipment_intensity", "op": "=", "value": "high"},
            "asset-heavy":             {"field": "equipment_intensity", "op": "=", "value": "high"},
            "asset-light":             {"field": "equipment_intensity", "op": "=", "value": "low"},
            "labor-only":              {"field": "equipment_intensity", "op": "=", "value": "low"},
        },
        "notes": (
            "FORWARD-looking: this table is who is ABOUT to recompete (period-of-performance end window), not who won. 'expiring / up for recompete / runway / performance ends' → days_until_expiry.",
            "NAICS vs PSC: naics2/naics_code = vendor industry; psc_category/psc_code = what the contract buys ('transportation services' / 'PSC V' → psc_category='V').",
            "Industry axes: naics2/naics_code = the RAW federal sector code; vertical = the 24-name"
            " GTM taxonomy over the (NAICS, PSC) pair. 'the <name> vertical/sector/industry'"
            " (e.g. 'aerospace contracts up for recompete', 'IT vertical expiring this quarter') →"
            " vertical, AND-ed with the days_until_expiry recompete window. work_type (make vs"
            " resell vs construct vs services vs RnD vs maintain_repair) and equipment_intensity"
            " (low/medium/high financing signal) are PEER filters. All three have partial coverage"
            " (~78% of recompete $, ~38% of rows — the unlabeled tail is small-dollar) — unlabeled"
            " rows surface in 'not applied', never silently filtered. what_was_done is a free-text"
            " DISPLAY field (shown for context, not filterable).",
        ),
        "aggregate": {
            "measure": "contract_current_value_usd",
            "dims": ["naics2", "naics_code", "psc_category", "psc_code", "awarding_agency",
                     "awarding_sub_agency", "state", "pop_state", "set_aside", "business_size",
                     "vertical", "work_type", "equipment_intensity"],
            "pseudo_dims": ["winner", "size_band"],
            "metrics": ["count", "sum", "avg", "median", "p90"],
            "desc": ("For BREAKDOWN / TOTAL / DISTRIBUTION / RANKING over expiring/active awards"
                     " ('expiring $ by agency', 'top incumbents up for recompete', 'recompete value"
                     " by PSC', 'distribution of expiring contract sizes'), ALSO emit an aggregate"
                     " object {group_by, metrics?, limit?}. group_by is a dim below, or 'winner'"
                     " (top incumbents by $) or 'size_band' (value histogram). The expiry window"
                     " STILL rides in filters via days_until_expiry. Omit aggregate for 'show me' rows."),
        },
    },
}


def _render_fields(d: dict) -> list[str]:
    lines = []
    for name, spec in d["fields"].items():
        bits = [f"- {name} ({spec['type']}) ops={list(spec['ops'])}"]
        if spec.get("enum"):
            bits.append(f"allowed values={list(spec['enum'])}")
        if spec.get("desc"):
            bits.append(f"— {spec['desc']}")
        lines.append(" ".join(bits))
    return lines


def _render_synonyms(d: dict) -> list[str]:
    return [f'- "{term}" → {clause}' for term, clause in d["synonyms"].items()]


def _render_aggregate(d: dict) -> list[str]:
    """Render the optional aggregate capability into the prompt (only when declared)."""
    agg = d.get("aggregate")
    if not agg:
        return []
    dims = list(agg["dims"]) + list(agg.get("pseudo_dims", []))
    return [
        "",
        f"Aggregate (optional): {agg['desc']}",
        f"- aggregate.group_by ∈ {dims}",
        f"- aggregate.metrics ⊆ {list(agg['metrics'])} (default: count, sum)",
    ]


def _aggregate_schema(agg: dict) -> dict:
    """The optional ``aggregate`` tool property — enum-bounded group_by + metrics (the
    first gate; EXECUTE's typed allowlist is authoritative)."""
    dims = list(agg["dims"]) + list(agg.get("pseudo_dims", []))
    return {
        "type": "object",
        "description": ("OPTIONAL — present ONLY for breakdown/total/distribution/ranking"
                        " queries; omit for row queries."),
        "properties": {
            "group_by": {"type": "string", "enum": dims,
                         "description": "dimension to group by (or 'winner' / 'size_band')"},
            "metrics": {"type": "array", "items": {"type": "string", "enum": list(agg["metrics"])},
                        "description": "metrics over the measure (default: count, sum)"},
            "limit": {"type": "integer", "description": "top-N groups (optional)"},
        },
        "required": ["group_by"],
    }


def _union_aggregate_schema() -> dict | None:
    """Union aggregate schema across every dataset that declares one (for the router tool).
    group_by/metrics enums are the union; EXECUTE re-validates against the routed dataset."""
    dims: list[str] = []
    mets: list[str] = []
    for d in DECODERS.values():
        agg = d.get("aggregate")
        if not agg:
            continue
        for v in list(agg["dims"]) + list(agg.get("pseudo_dims", [])):
            if v not in dims:
                dims.append(v)
        for m in agg["metrics"]:
            if m not in mets:
                mets.append(m)
    if not dims:
        return None
    return _aggregate_schema({"dims": dims, "pseudo_dims": [], "metrics": mets})


_OUTPUT_RULES = (
    "- Use ONLY the listed fields and their listed ops. For an enum field use only its allowed values.",
    "- Numeric value for >= and <=; [lo, hi] for between; an array for in; bare true/false for bool.",
    "- A days_ago / days_ahead field takes a whole-day INTEGER count, never a calendar date.",
    "- Combine multiple conditions as separate filter clauses (they are AND-combined).",
    "- NEVER silently drop part of the query. Any constraint you cannot express with the"
    " listed fields goes into the unmapped array as a short verbatim phrase from the query.",
    "- If the query implies no usable filter, return an empty filters array (and record"
    " whatever you could not map in unmapped).",
    "- If everything mapped, return an empty unmapped array.",
)


def render_decoder_prompt(dataset: str) -> str:
    """The cached system block: what the table is, the allowlisted fields with types +
    ops (+ enum/desc), the known phrase→filter rows, and the hard output rules."""
    d = DECODERS[dataset]
    lines = [
        f"You translate a natural-language map query into a constrained filter for: {d['description']}",
        "",
        "Allowed fields (use ONLY these; choose the field whose meaning matches the query):",
        *_render_fields(d),
        "",
        "Known phrase → filter mappings (apply when the phrase appears):",
        *_render_synonyms(d),
        *_render_aggregate(d),
        "",
        "Rules:",
        "- Emit ONLY via the emit_filter tool. Never prose.",
        *_OUTPUT_RULES,
    ]
    lines += [f"- {note}" for note in d.get("notes", ())]
    return "\n".join(lines)


def _filters_schema(field_names: list[str]) -> dict:
    return {
        "type": "array",
        "description": "AND-combined filter clauses",
        "items": {
            "type": "object",
            "properties": {
                "field": {"type": "string", "enum": field_names},
                "op": {"type": "string", "enum": list(OPS)},
                "value": {"description": "scalar for =,>=,<=; array for in/between; true/false for bool; whole-day integer for days_ago"},
            },
            "required": ["field", "op", "value"],
        },
    }


# The honesty contract: a constraint the allowlist cannot express is SURFACED,
# never silently dropped — the UI renders these as "not applied".
_UNMAPPED_SCHEMA = {
    "type": "array",
    "items": {"type": "string"},
    "description": "verbatim phrases from the query that could NOT be mapped to any allowed field (empty when everything mapped)",
}


def build_emit_filter_tool(dataset: str) -> dict:
    """The forced tool: ``field`` + ``op`` are enum-bounded at the schema level (the
    first gate; EXECUTE's typed allowlist is the authoritative one)."""
    d = DECODERS[dataset]
    props = {
        "title": {"type": "string", "description": "a short human label for the query"},
        "filters": _filters_schema(list(d["fields"])),
        "unmapped": _UNMAPPED_SCHEMA,
    }
    if d.get("aggregate"):
        props["aggregate"] = _aggregate_schema(d["aggregate"])   # optional — not in `required`
    return {
        "name": "emit_filter",
        "description": "Translate the user's map query into a constrained filter object.",
        "input_schema": {
            "type": "object",
            "properties": props,
            "required": ["title", "filters", "unmapped"],
        },
    }


# ── Dataset routing (the AUTO surface) ────────────────────────────────────────
# One forced-tool call picks the dataset AND compiles its filters. Bump on any
# change to the routing rules below; combined with every per-dataset version in
# the auto memo key so any axis change busts cached routings.
ROUTER_VERSION = "router.v8"   # v7→v8: awards gains the GTM label axes vertical/work_type/
                               # equipment_intensity — the awards routing cue now steers industry/
                               # vertical/sector + make-vs-resell + equipment-intensity phrasing.
                               # v6→v7: awards gains the option-exercise / action_type axis; drop a
                               # stale 'awards is the only aggregate dataset' claim (all 4 aggregate)
                               # v5→v6: winners + company gain the aggregate capability
                               # v4→v5: add the 'active' dataset (forward-looking recompete radar)
                               # v3→v4: aggregate capability rendered into the router prompt + tool
                               # v2→v3: awards routing cue gains the PSC / transportation-services axis

# Routing cues rendered into the router system block, per dataset.
_ROUTING_CUES = {
    "awards": "AWARD-EVENT questions: 'won an award/contract over $X', 'awards in the last"
              " N days', 'who won <agency> contracts this week', set-asides, place of"
              " performance, PSC (what the contract BUYS — 'transportation/freight SERVICES',"
              " 'PSC category V'), and OPTION EXERCISES (the govt exercising a contract option"
              " = a mobilization event — 'options exercised', 'mobilization'). ALSO the GTM LABEL"
              " axes on the (NAICS, PSC) pair: INDUSTRY / VERTICAL / SECTOR phrasing ('the IT"
              " vertical', 'aerospace sector', 'healthcare industry') → vertical; make-vs-resell"
              " ('manufacturers' vs 'resellers/distributors') → work_type; EQUIPMENT-HEAVY /"
              " capital-intensity → equipment_intensity. DEFAULT for win/award phrasing.",
    "company": "COMPANY-ATTRIBUTE questions: lifetime/active federal obligations,"
               " firmographics (employee size, founded year, industry label, company type),"
               " SAM registration, 'federal contractors with $X total obligations'.",
    "winners": "PER-WINNER ROLLUPS: 'entities whose total won exceeds $X', aggregate"
               " award_count. ALSO the SUBAWARDEE"
               " TEAMING axis: 'subs that have teamed with <prime>', '$X+ teaming', 'subs that"
               " teamed with N+ primes'. ALSO the SUBAWARDEE SELF-REPORTED axis: 'subs that"
               " self-report <capability>', 'subs with an ISO 9001 / CMMC / AS9100 certification'.",
    "active": "FORWARD-LOOKING / RECOMPETE questions: contracts EXPIRING / up for recompete /"
              " whose period of performance ends in the next N days, runway left, incumbents about"
              " to lose a contract. DEFAULT for 'expiring', 'recompete', 'renewal', 'runway',"
              " 'active contracts ending'. ALSO carries the GTM LABEL axes (INDUSTRY / VERTICAL /"
              " SECTOR → vertical; make-vs-resell → work_type; equipment-heavy → equipment_intensity):"
              " a vertical/work_type/equipment clause AND-ed with an expiring/recompete window"
              " ('aerospace contracts up for recompete', 'IT vertical expiring this quarter') routes"
              " HERE, not awards. (awards = who already won; active = who's about to rebuy.)",
}


def router_memo_version() -> str:
    """The auto-translate memo version: the router prompt + every dataset axis."""
    return ROUTER_VERSION + "|" + "|".join(d["version"] for d in DECODERS.values())


def render_router_prompt() -> str:
    """The cached system block for dataset-AUTO translate: every dataset's description,
    routing cues, field axis and synonyms — the model picks ONE dataset and compiles
    the filters for THAT dataset only."""
    lines = [
        "You route a natural-language market query to ONE dataset and translate it into a"
        " constrained filter for that dataset.",
        "",
        "Datasets:",
    ]
    for key, d in DECODERS.items():
        lines += [
            "",
            f"### dataset = {key!r} — {d['description']}",
            f"Route here for: {_ROUTING_CUES[key]}",
            "Fields:",
            *_render_fields(d),
            "Phrase → filter mappings:",
            *_render_synonyms(d),
            *_render_aggregate(d),
        ]
        lines += [f"Note: {note}" for note in d.get("notes", ())]
    lines += [
        "",
        "Rules:",
        "- Emit ONLY via the emit_filter tool. Never prose.",
        "- Pick exactly ONE dataset; every filter clause must use FIELDS OF THAT DATASET.",
        *_OUTPUT_RULES,
    ]
    return "\n".join(lines)


def reconcile_routed_filters(filt: dict) -> dict:
    """Post-route honesty pass: the router tool's ``field`` enum is the UNION of every
    dataset's fields, so a clause can name a field the ROUTED dataset does not carry.
    Such a clause must not 422 the whole query NOR run silently mutated — it moves to
    ``unmapped`` (rendered as "not applied"). EXECUTE's typed allowlist stays the
    authoritative gate for everything kept. Pure (no I/O) — unit-tested directly."""
    dataset = filt.get("dataset")
    if dataset not in DECODERS:
        filt["dataset"] = dataset = "awards"   # forced enum makes this unreachable; belt+braces
    fields = DECODERS[dataset]["fields"]
    kept: list = []
    moved: list[str] = []
    for clause in filt.get("filters") or []:
        field = clause.get("field") if isinstance(clause, dict) else None
        if field in fields and clause.get("op") in fields[field]["ops"]:
            kept.append(clause)
        else:
            moved.append(
                f"{field} {clause.get('op')} {clause.get('value')}"
                if isinstance(clause, dict) else str(clause)
            )
    filt["filters"] = kept
    # An aggregate emitted while routed to a dataset that does NOT support aggregation cannot
    # run — surface it (honesty contract), never execute it silently against the wrong table.
    if filt.get("aggregate") and not DECODERS[dataset].get("aggregate"):
        agg = filt.pop("aggregate")
        gb = agg.get("group_by") if isinstance(agg, dict) else agg
        moved.append(f"aggregate by {gb} (not supported for dataset {dataset})")
    if moved:
        filt["unmapped"] = list(filt.get("unmapped") or []) + moved
    return filt


def build_router_tool() -> dict:
    """The forced routing tool: ``dataset`` is enum-bounded; ``field`` is the UNION of
    every dataset's fields (the per-dataset check happens after routing — edge moves
    any clause off the routed dataset's axis into ``unmapped``, and EXECUTE's typed
    allowlist remains the authoritative gate)."""
    union: list[str] = []
    for d in DECODERS.values():
        for name in d["fields"]:
            if name not in union:
                union.append(name)
    props = {
        "dataset": {"type": "string", "enum": list(DECODERS),
                    "description": "the ONE dataset this query runs against"},
        "title": {"type": "string", "description": "a short human label for the query"},
        "filters": _filters_schema(union),
        "unmapped": _UNMAPPED_SCHEMA,
    }
    agg_schema = _union_aggregate_schema()
    if agg_schema is not None:
        # Optional; only valid when the routed dataset declares an aggregate (reconcile drops
        # it otherwise). EXECUTE re-validates group_by/metrics against the routed dataset.
        agg_schema = {**agg_schema, "description": agg_schema["description"]
                      + " Valid only when dataset routes to one that supports aggregation."}
        props["aggregate"] = agg_schema
    return {
        "name": "emit_filter",
        "description": "Route the user's market query to one dataset and translate it into a constrained filter object.",
        "input_schema": {
            "type": "object",
            "properties": props,
            "required": ["dataset", "title", "filters", "unmapped"],
        },
    }
