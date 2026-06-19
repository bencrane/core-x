"""Map query decoders for catalyst_api — the field/op/type allowlist that turns a
compiled filter object into a safe Lance scanner predicate.

One decoder per map serving table. This is the load-bearing security artifact: the
EXECUTE path (``lance_store.compile_map_filter``) rejects any field/op/value not
declared here, and column names come ONLY from ``FieldSpec.column`` (never from the
caller). The prompt-facing subset (field names, ops, enums, synonyms) is what
``edge_api`` renders into the ``emit_filter`` tool schema in TRANSLATE (Phase 3).

Bump a decoder's ``version`` on ANY change to its fields/enums/synonyms — it is the
cache-busting key for edge_api's translation memo.

Field types are verified against the live serving-table Arrow schemas: every string
column is ``string``; ``total_obligation``/``total_active_obligations`` are ``double``;
``award_count`` is ``int64``; ``founded_year`` is ``int32``; ``has_federal_awards`` and
``is_active`` are ``bool``.

The ``days_ago`` field type is the RELATIVE-TIME axis: the caller supplies a whole-day
count and EXECUTE resolves it against ``date.today()`` at request time, compiling to a
``DATE 'YYYY-MM-DD'`` literal over the underlying action-date column (date32 on company,
ISO string on winners — DataFusion coerces both, verified live). Resolving the date at
EXECUTE time — not at TRANSLATE time — keeps edge_api's translation memo safe: a cached
"this week" sentence re-resolves to the current week on every execution.
"""
from __future__ import annotations

from dataclasses import dataclass

# Global op enum (matches NL_QUERY_MAP_COMPILER_STRATEGY.md). A field's own `ops`
# tuple is a subset of this; the compiler rejects anything outside it. `has`/`has_any`
# are the list-membership ops (PHASE 3) — valid only on `type="list"` fields, compiled
# to Lance `array_has` / `array_has_any`.
OPS = ("=", ">=", "<=", "in", "between", "has", "has_any")


@dataclass(frozen=True)
class FieldSpec:
    column: str                       # hardcoded Lance column (NEVER from the caller)
    type: str                         # "string" | "int" | "float" | "bool" | "days_ago" | "list"
    ops: tuple[str, ...]              # ops valid for THIS field (subset of OPS)
    enum: tuple | None = None         # allowed values; None = open-valued (still type-checked)
    index: str | None = None          # "BTREE" | "BITMAP" | None — observability only
    gated: bool = False               # PHASE 3: capability axis — EXECUTE ANDs has_extracted_scope=true


@dataclass(frozen=True)
class Decoder:
    dataset_key: str                  # key into config.MAP_DATASET_URIS
    version: str                      # bump on any field/enum/synonym change
    geometry: tuple[str, str]         # (lon_col, lat_col)
    properties: tuple[str, ...]       # thin property set emitted per GeoJSON feature
    fields: dict[str, FieldSpec]      # query-name -> spec
    synonyms: dict[str, dict]         # NL term -> {"field","op","value"} (canned + prompt rows)


# ── PHASE-3 capability controlled vocabularies (frozen from govcon_award_capability_profiles,
# probed live 2026-06-14). These bound the TRANSLATE output space and EXECUTE enum-checks.
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

# ── Teaming prime canonical-name sets (probed live 2026-06-19 from
# govcon_subawardee_capability_profiles.teaming_prime_names; stored as full UPPERCASE legal
# names, one company → several variants). array_has needs the EXACT element, so a "lockheed"
# synonym maps to has_any over the observed corporate-entity variants. Subsidiaries that carry a
# distinct brand are kept out (e.g. Lockheed Sippican/Services) — the synonym is the parent family.
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

WINNERS = Decoder(
    dataset_key="winners",
    version="winners.v4",   # v3→v4: SUB-only teaming axis (dollars/primes/prime-name list)
    geometry=("longitude", "latitude"),
    properties=("winner_uei", "winner_name", "winner_type", "naics_code", "naics2",
                "state", "total_obligation", "award_count", "last_action_date",
                # PHASE-3 capability surface (structured only — NO chunk-derived verbatim text)
                "has_extracted_scope", "requires_clearance", "req_clearance_level_max",
                "requires_cmmc", "capability_tags", "labor_categories",
                "covered_award_count", "covered_award_keys",
                # SUB-only teaming surface (null on prime rows)
                "teaming_dollars_5y", "n_teaming_primes", "teaming_prime_names"),
    fields={
        "naics2":           FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "state":            FieldSpec("state", "string", ("=", "in"), index="BITMAP"),
        "winner_type":      FieldSpec("winner_type", "string", ("=", "in"),
                                      enum=("prime_recipient", "subawardee"), index="BITMAP"),
        "naics_code":       FieldSpec("naics_code", "string", ("=", "in")),
        "total_obligation": FieldSpec("total_obligation", "float", (">=", "<=", "between")),
        "award_count":      FieldSpec("award_count", "int", (">=", "<=", "between")),
        # ISO-string action date; days_ago resolves to a DATE literal at request time.
        "days_since_last_award": FieldSpec("last_action_date", "days_ago", ("<=", ">=", "between")),
        # ── PHASE-3 capability axis (gated: EXECUTE ANDs has_extracted_scope=true) ──
        # has_extracted_scope is the gate itself — filterable but NOT gated (no self-AND).
        "has_extracted_scope": FieldSpec("has_extracted_scope", "bool", ("=",), index="BITMAP"),
        "requires_clearance":  FieldSpec("requires_clearance", "bool", ("=",), index="BITMAP",
                                         gated=True),
        "req_clearance_level_max": FieldSpec("req_clearance_level_max", "string", ("=", "in"),
                                             enum=_CLEARANCE_LEVELS, index="BITMAP", gated=True),
        "requires_cmmc":       FieldSpec("requires_cmmc", "bool", ("=",), index="BITMAP", gated=True),
        # list<string> capability columns — set membership via Lance array_has / array_has_any.
        "capability_tag":      FieldSpec("capability_tags", "list", ("has", "has_any"),
                                         enum=_CAPABILITY_TAGS, gated=True),
        "labor_category":      FieldSpec("labor_categories", "list", ("has", "has_any"),
                                         enum=_LABOR_CATEGORIES, gated=True),
        # ── SUB-only teaming axis (NOT gated: teaming is on every profiled sub, not the
        # scope-extracted slice — do not self-AND has_extracted_scope). Null on prime rows. ──
        "teaming_dollars_5y":  FieldSpec("teaming_dollars_5y", "float", (">=", "<=", "between"),
                                         index="BTREE"),
        "n_teaming_primes":    FieldSpec("n_teaming_primes", "int", (">=", "<=", "between"),
                                         index="BTREE"),
        # Exact prime legal names (open-valued — the synonyms below map common primes to their
        # observed canonical variant sets, since array_has needs the exact stored element).
        "teaming_prime":       FieldSpec("teaming_prime_names", "list", ("has", "has_any")),
    },
    synonyms={
        "construction": {"field": "naics2", "op": "=", "value": "23"},
        "subawards":    {"field": "winner_type", "op": "=", "value": "subawardee"},
        "prime awards": {"field": "winner_type", "op": "=", "value": "prime_recipient"},
        "this week":    {"field": "days_since_last_award", "op": "<=", "value": 7},
        "won recently": {"field": "days_since_last_award", "op": "<=", "value": 30},
        # PHASE-3 capability phrasings
        "cleared":          {"field": "requires_clearance", "op": "=", "value": True},
        "secret clearance": {"field": "req_clearance_level_max", "op": "in",
                             "value": ["SECRET", "TOP_SECRET", "TS_SCI"]},
        "top secret":       {"field": "req_clearance_level_max", "op": "in",
                             "value": ["TOP_SECRET", "TS_SCI"]},
        "cmmc":             {"field": "requires_cmmc", "op": "=", "value": True},
        "electrical":       {"field": "capability_tag", "op": "has", "value": "electrical_systems"},
        "electricians":     {"field": "labor_category", "op": "has", "value": "electrician"},
        # SUB-only teaming phrasings. Prime names are exact stored legal names → has_any over the
        # observed canonical variant set (so "teamed with lockheed" hits every Lockheed entity).
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
    },
)


COMPANY = Decoder(
    dataset_key="company",
    version="company.v3",
    geometry=("longitude", "latitude"),
    properties=("uei", "company_name", "industry", "employee_size_band", "company_type",
                "naics2", "primary_naics", "hq_city", "hq_state", "has_federal_awards",
                "total_active_obligations", "award_count", "latest_award_action_date"),
    fields={
        "naics2":             FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "industry":           FieldSpec("industry", "string", ("=", "in"), index="BITMAP"),
        "employee_size_band": FieldSpec("employee_size_band", "string", ("=", "in"),
                                        enum=("1-10", "11-50", "51-200", "201-500", "501-1000",
                                              "1001-5000", "5001-10000", "10001+"), index="BITMAP"),
        "company_type":       FieldSpec("company_type", "string", ("=", "in"),
                                        enum=("Educational", "Educational Institution", "Government Agency",
                                              "Nonprofit", "Partnership", "Privately Held", "Public Company",
                                              "Self-Employed", "Self-Owned", "Sole Proprietorship"), index="BITMAP"),
        # query-name `state` maps to the indexed column `physical_address_state`
        "state":              FieldSpec("physical_address_state", "string", ("=", "in"), index="BITMAP"),
        "has_federal_awards": FieldSpec("has_federal_awards", "bool", ("=",), index="BITMAP"),
        "is_active":          FieldSpec("is_active", "bool", ("=",)),
        "primary_naics":      FieldSpec("primary_naics", "string", ("=", "in"), index="BTREE"),
        "founded_year":       FieldSpec("founded_year", "int", (">=", "<=", "between")),
        "active_obligations": FieldSpec("total_active_obligations", "float", (">=", "<=", "between")),
        "award_count":        FieldSpec("award_count", "int", (">=", "<=", "between")),
        # date32 most-recent prime/subaward action date (materialize_company_map.py recency
        # join); days_ago resolves to a DATE literal at request time. BTREE serves the ranges.
        "days_since_last_award": FieldSpec("latest_award_action_date", "days_ago",
                                           ("<=", ">=", "between"), index="BTREE"),
    },
    synonyms={
        "construction":        {"field": "naics2", "op": "=", "value": "23"},
        "federal contractors": {"field": "has_federal_awards", "op": "=", "value": True},
        "active":              {"field": "is_active", "op": "=", "value": True},
        "this week":           {"field": "days_since_last_award", "op": "<=", "value": 7},
        "won recently":        {"field": "days_since_last_award", "op": "<=", "value": 30},
    },
)


# Set-aside codes verified live against the built serving table (18 distinct; 'NONE'
# means the action was explicitly competed without a set-aside — distinct from NULL,
# which means the source row carried no set-aside signal at all).
_SET_ASIDE_CODES = ("NONE", "SBA", "SBP", "8A", "8AN", "SDVOSBC", "SDVOSBS", "WOSB",
                    "WOSBSS", "EDWOSB", "EDWOSBSS", "HZC", "HZS", "ISBEE", "BI", "IEE",
                    "VSA", "VSS")

AWARDS = Decoder(
    dataset_key="awards",
    version="awards.v1",
    geometry=("longitude", "latitude"),
    properties=("award_id", "winner_uei", "winner_name", "winner_type", "award_amount",
                "action_date", "naics2", "naics_code", "state", "city", "county",
                "pop_state", "pop_city", "awarding_agency", "awarding_sub_agency",
                "set_aside"),
    fields={
        # The single action's obligation — NEVER a lifetime or window rollup. The build
        # excludes de-obligations and $0 admin mods, so ">= X" is honest "won" semantics.
        "award_amount":      FieldSpec("award_amount", "float", (">=", "<=", "between"), index="BTREE"),
        "days_since_action": FieldSpec("action_date", "days_ago", ("<=", ">=", "between"), index="BTREE"),
        "naics2":            FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "naics_code":        FieldSpec("naics_code", "string", ("=", "in")),
        # Recipient (HQ) geo vs PLACE OF PERFORMANCE geo — two distinct axes by design.
        "state":             FieldSpec("state", "string", ("=", "in"), index="BITMAP"),
        "city":              FieldSpec("city", "string", ("=", "in"), index="BTREE"),
        "county":            FieldSpec("county", "string", ("=", "in"), index="BTREE"),
        "pop_state":         FieldSpec("pop_state", "string", ("=", "in"), index="BITMAP"),
        "pop_city":          FieldSpec("pop_city", "string", ("=", "in"), index="BTREE"),
        "awarding_agency":   FieldSpec("awarding_agency", "string", ("=", "in"), index="BITMAP"),
        "awarding_sub_agency": FieldSpec("awarding_sub_agency", "string", ("=", "in"), index="BTREE"),
        "set_aside":         FieldSpec("set_aside", "string", ("=", "in"),
                                       enum=_SET_ASIDE_CODES, index="BITMAP"),
        "winner_type":       FieldSpec("winner_type", "string", ("=", "in"),
                                       enum=("prime_recipient", "subawardee"), index="BITMAP"),
    },
    synonyms={
        "construction":   {"field": "naics2", "op": "=", "value": "23"},
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
    },
)


DECODERS: dict[str, Decoder] = {"winners": WINNERS, "company": COMPANY, "awards": AWARDS}
