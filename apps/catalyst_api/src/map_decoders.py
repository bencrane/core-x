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


# ── Aggregate capability (the GROUP-BY allowlist, authoritative) ──────────────
# An AGGREGATE answers "how much / distribution / breakdown / top-N" over the SAME
# compiled filter predicate the row path uses — so the time window stays QUERY-DRIVEN
# (a days_since_action clause), never a hardcoded lookback. EXECUTE (lance_store.
# map_aggregate) rejects any dim/metric not declared here; the group/measure COLUMNS
# come ONLY from this spec, never from the caller. Metrics are computed over `measure`
# (e.g. award_amount) with pyarrow hash-aggregates (no SQL engine in EXECUTE).
AGG_METRICS = ("count", "sum", "avg", "median", "p90")  # over the measure column

@dataclass(frozen=True)
class AggregateSpec:
    measure: str                      # hardcoded numeric column the metrics aggregate (e.g. award_amount)
    dims: dict[str, str]              # group-by query-name -> hardcoded column (BITMAP/BTREE indexed)
    metrics: tuple[str, ...] = AGG_METRICS
    # Pseudo-dims computed at EXECUTE (not raw columns): 'winner' (top entities by measure)
    # and 'size_band' (a measure histogram). winner_key = (uei_col, name_col).
    winner_key: tuple[str, str] | None = None
    size_band_edges: tuple[float, ...] = ()   # ascending bucket boundaries for 'size_band'
    default_limit: int = 25           # top-N groups returned (ordered by the primary metric desc)
    max_limit: int = 500


@dataclass(frozen=True)
class Decoder:
    dataset_key: str                  # key into config.MAP_DATASET_URIS
    version: str                      # bump on any field/enum/synonym change
    geometry: tuple[str, str]         # (lon_col, lat_col)
    properties: tuple[str, ...]       # thin property set emitted per GeoJSON feature
    fields: dict[str, FieldSpec]      # query-name -> spec
    synonyms: dict[str, dict]         # NL term -> {"field","op","value"} (canned + prompt rows)
    aggregate: "AggregateSpec | None" = None   # GROUP-BY allowlist; None = aggregation unsupported


# ── PHASE-3 capability controlled vocabularies (frozen from govcon_award_solicitation_profiles,
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
# govcon_subawardee_profiles.teaming_prime_names; stored as full UPPERCASE legal
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

# ── Cert-token canonical sets for the SUB-only req_cert_tags axis (probed live 2026-06-19 from
# govcon_subawardee_profiles.req_cert_tags — a 172-distinct OPEN vocabulary, too
# granular/niche for an enum). The field stays open-valued; these synonyms map common cert
# phrasings to has_any over the EXACT stored tokens (array_has needs the exact list element).
# Every token below was confirmed present in the live column.
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

WINNERS = Decoder(
    dataset_key="winners",
    version="winners.v8",   # v7→v8: add the AGGREGATE capability (rollup total_obligation by dim / top winners / distribution)
                            # v6→v7: drop stale ~90-day window claim from prompt-facing copy (data spans full history)
    geometry=("longitude", "latitude"),
    properties=("winner_uei", "winner_name", "winner_type", "naics_code", "naics2",
                "state", "total_obligation", "award_count", "last_action_date",
                # PHASE-3 capability surface (structured only — NO chunk-derived verbatim text)
                "has_extracted_scope", "requires_clearance", "req_clearance_level_max",
                "requires_cmmc", "solicitation_scope_tags", "labor_categories",
                "covered_award_count", "covered_award_keys",
                # SUB-only teaming surface (null on prime rows)
                "teaming_dollars_5y", "n_teaming_primes", "teaming_prime_names",
                # SUB-only self-reported surface (null on prime rows + on subs with no signal)
                "subaward_description_tags", "req_cert_tags"),
    fields={
        "naics2":           FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "state":            FieldSpec("state", "string", ("=", "in"), index="BITMAP"),
        "winner_type":      FieldSpec("winner_type", "string", ("=", "in"),
                                      enum=("prime_recipient", "subawardee"), index="BITMAP"),
        "naics_code":       FieldSpec("naics_code", "string", ("=", "in")),
        "total_obligation": FieldSpec("total_obligation", "float", (">=", "<=", "between"), index="BTREE"),
        "award_count":      FieldSpec("award_count", "int", (">=", "<=", "between"), index="BTREE"),
        # ISO-string action date; days_ago resolves to a DATE literal at request time. BTREE serves ranges.
        "days_since_last_award": FieldSpec("last_action_date", "days_ago", ("<=", ">=", "between"), index="BTREE"),
        # ── PHASE-3 capability axis (gated: EXECUTE ANDs has_extracted_scope=true) ──
        # has_extracted_scope is the gate itself — filterable but NOT gated (no self-AND).
        "has_extracted_scope": FieldSpec("has_extracted_scope", "bool", ("=",), index="BITMAP"),
        "requires_clearance":  FieldSpec("requires_clearance", "bool", ("=",), index="BITMAP",
                                         gated=True),
        "req_clearance_level_max": FieldSpec("req_clearance_level_max", "string", ("=", "in"),
                                             enum=_CLEARANCE_LEVELS, index="BITMAP", gated=True),
        "requires_cmmc":       FieldSpec("requires_cmmc", "bool", ("=",), index="BITMAP", gated=True),
        # list<string> capability columns — set membership via Lance array_has / array_has_any.
        "solicitation_scope_tag":      FieldSpec("solicitation_scope_tags", "list", ("has", "has_any"),
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
        # ── SUB-only SELF-REPORTED axis (NOT gated: 13,792 subs self-report capability vs. the
        # ~4,220 scope-extracted slice the gated solicitation_scope_tag axis reaches — gating would defeat
        # the long-tail purpose). Distinct, ADDITIONAL field; does NOT replace gated solicitation_scope_tag.
        # Same 77-tag controlled vocab → reuse _CAPABILITY_TAGS for the enum. Null on prime rows. ──
        "subaward_description_tag": FieldSpec("subaward_description_tags", "list",
                                                  ("has", "has_any"), enum=_CAPABILITY_TAGS),
        # Certification tokens (open-valued — 172-distinct granular vocab; the synonyms below map
        # common cert phrasings to has_any over the exact stored tokens). Null on prime rows.
        "req_cert_tag":        FieldSpec("req_cert_tags", "list", ("has", "has_any")),
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
        "electrical":       {"field": "solicitation_scope_tag", "op": "has", "value": "electrical_systems"},
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
        # ── SUB-only SELF-REPORTED phrasings. These route the "self-report"/"self-reported" cue to
        # the UNGATED subaward_description_tag field — the distinct long-tail axis, NOT the gated
        # solicitation_scope_tag axis (whose own synonyms electrical/electricians are left untouched). ──
        "self-report software development":   {"field": "subaward_description_tag", "op": "has", "value": "software_development"},
        "self-reported software development": {"field": "subaward_description_tag", "op": "has", "value": "software_development"},
        "self-report aircraft maintenance":   {"field": "subaward_description_tag", "op": "has", "value": "aircraft_maintenance"},
        "self-reported aircraft maintenance": {"field": "subaward_description_tag", "op": "has", "value": "aircraft_maintenance"},
        # ── Certification phrasings → has_any over the EXACT stored cert tokens (open vocab). ──
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
    # Aggregate over the per-winner rolled-up obligation. group-by any indexed dim (or 'winner'
    # top-N / 'size_band' histogram); measure = total_obligation.
    aggregate=AggregateSpec(
        measure="total_obligation",
        dims={"naics2": "naics2", "naics_code": "naics_code", "state": "state",
              "winner_type": "winner_type"},
        winner_key=("winner_uei", "winner_name"),
        size_band_edges=(100_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0, 1_000_000_000.0),
    ),
)


COMPANY = Decoder(
    dataset_key="company",
    version="company.v4",   # v3→v4: add the AGGREGATE capability (rollup active obligations by firmographic dim)
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
        "is_active":          FieldSpec("is_active", "bool", ("=",), index="BITMAP"),
        "primary_naics":      FieldSpec("primary_naics", "string", ("=", "in"), index="BTREE"),
        "founded_year":       FieldSpec("founded_year", "int", (">=", "<=", "between"), index="BTREE"),
        "active_obligations": FieldSpec("total_active_obligations", "float", (">=", "<=", "between"), index="BTREE"),
        "award_count":        FieldSpec("award_count", "int", (">=", "<=", "between"), index="BTREE"),
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
    # Aggregate over total active federal obligations. group-by any firmographic dim (or 'winner'
    # top-N companies / 'size_band' histogram); measure = total_active_obligations.
    aggregate=AggregateSpec(
        measure="total_active_obligations",
        dims={"naics2": "naics2", "industry": "industry", "employee_size_band": "employee_size_band",
              "company_type": "company_type", "state": "physical_address_state",
              "primary_naics": "primary_naics"},
        winner_key=("uei", "company_name"),
        size_band_edges=(100_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0, 1_000_000_000.0),
    ),
)


# Set-aside codes verified live against the built serving table (18 distinct; 'NONE'
# means the action was explicitly competed without a set-aside — distinct from NULL,
# which means the source row carried no set-aside signal at all).
_SET_ASIDE_CODES = ("NONE", "SBA", "SBP", "8A", "8AN", "SDVOSBC", "SDVOSBS", "WOSB",
                    "WOSBSS", "EDWOSB", "EDWOSBSS", "HZC", "HZS", "ISBEE", "BI", "IEE",
                    "VSA", "VSS")
# FPDS contracting-officer business-size determination (shared by awards + active decoders).
_BUSINESS_SIZE = ("SMALL BUSINESS", "OTHER THAN SMALL BUSINESS")

# ── GTM-attribute label axes materialized onto the awards serving table (PR #715 vertical
# labels, PR #720 what_was_done display). All three live as BITMAP columns. Head-coverage only:
# the 279 top-$ (naics_code, psc_code) pairs ≈ 80% of both-codes $ but ~35% of rows — unlabeled
# rows stay HONESTLY unmatched on these axes (never silently filtered). 23 of 24 verticals are
# present in the head; "Staffing & Human Capital" is in-taxonomy at 0 labeled rows. The enum
# strings are byte-exact against pipelines/reference/data/naics_psc_top279_classified.csv — note
# the embedded COMMAS inside "Facilities, Maintenance & Janitorial" and "Food, Agriculture &
# Beverage" (a wrong comma/& → zero rows on a Lance BITMAP scan). MUST stay byte-identical to the
# edge_api mirror (the parity test asserts the enum value-sets match edge↔catalyst).
_VERTICALS = (
    "Information Technology & Software", "Aerospace & Defense", "Construction",
    "Research & Development", "Professional & Management Services", "Healthcare & Life Sciences",
    "Facilities, Maintenance & Janitorial", "Engineering & Architecture", "Transportation & Logistics",
    "Food, Agriculture & Beverage", "Wholesale & Supply", "Environmental & Remediation",
    "Electronics & Instruments", "Telecommunications", "Energy & Utilities", "Industrial Manufacturing",
    "Financial & Insurance", "Security & Guard Services", "Government & Public Administration",
    "Education & Training", "Real Estate", "Media & Publishing", "Mining & Extraction",
    "Staffing & Human Capital")
# What the vendor DOES with the (naics, psc) pair — the make-vs-resell axis. manufacture/construct
# front-load capital; distribute_resell does not — the mobilization-capital signal.
_WORK_TYPES = ("services_labor", "manufacture", "distribute_resell", "construct", "RnD", "maintain_repair")
# Capital intensity of the work — the equipment-financing-need proxy.
_EQUIP_INTENSITY = ("low", "medium", "high")

AWARDS = Decoder(
    dataset_key="awards",
    version="awards.v9",   # v8→v9: expose the GTM-attribute label axes vertical/work_type/
                           # equipment_intensity (filter fields + aggregate dims, all BITMAP) and
                           # carry what_was_done as a self-describing DISPLAY property (PR #715
                           # vertical labels, #720 what_was_done gloss). Head-coverage only.
                           # v7→v8: add the action_type axis + is_option_exercise flag — the FPDS
                           # 'EXERCISE AN OPTION' event = a mobilization-capital trigger on an
                           # already-invested contract (award_amount = the mobilization $).
                           # v6→v7: add the business_size axis (small vs other-than-small) — the
                           # contracting officer's determination; prime-only.
                           # v5→v6: add the fiscal_year axis (US federal FY of the action) — YoY
                           # spend trend via group-by; explicit-year filter ('FY2025' → 2025).
                           # v4→v5: add the AGGREGATE capability (group-by + count/sum/avg/median/p90,
                           # size-band histogram, top-N winners) over the same query-driven filter.
                           # v3→v4: add the PSC axis (psc_category/psc_code) — product/service
                           # bought, distinct from NAICS; surfaces transport/freight services
                           # (PSC 'V') coded under a non-48/49 NAICS that a NAICS-only filter misses.
    geometry=("longitude", "latitude"),
    properties=("award_id", "winner_uei", "winner_name", "winner_type", "award_amount",
                "action_date", "fiscal_year", "naics2", "naics_code", "psc_category", "psc_code",
                "state", "city", "county",
                "pop_state", "pop_city", "awarding_agency", "awarding_sub_agency",
                "set_aside", "business_size", "action_type", "is_option_exercise",
                "is_active", "pop_end",
                # GTM-attribute label axes + the free-text gloss. what_was_done is DISPLAY-only —
                # NOT a filter field, NOT indexed (it rides here so the feature self-describes).
                "vertical", "work_type", "equipment_intensity", "what_was_done"),
    fields={
        # The single action's obligation — NEVER a lifetime or window rollup. The build
        # excludes de-obligations and $0 admin mods, so ">= X" is honest "won" semantics.
        "award_amount":      FieldSpec("award_amount", "float", (">=", "<=", "between"), index="BTREE"),
        "days_since_action": FieldSpec("action_date", "days_ago", ("<=", ">=", "between"), index="BTREE"),
        # US federal fiscal year of the action (Oct–Sep). Explicit year only ('FY2025' → 2025);
        # group-by fiscal_year is the YoY spend trend. A relative window uses days_since_action.
        "fiscal_year":       FieldSpec("fiscal_year", "int", ("=", "in"), index="BITMAP"),
        "naics2":            FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "naics_code":        FieldSpec("naics_code", "string", ("=", "in")),
        # ── GTM-attribute axes (PR #715/#720): the award's (naics, psc) pair classified into a
        # rich VERTICAL (24-name taxonomy, distinct from the raw naics2 sector), a WORK_TYPE
        # (make vs resell vs build — the mobilization-capital signal), and an EQUIPMENT_INTENSITY
        # band (financing proxy). All three BITMAP, head-coverage only; peer filters that AND with
        # naics2/psc. The free-text what_was_done gloss is a DISPLAY property (see properties). ──
        "vertical":          FieldSpec("vertical", "string", ("=", "in"), enum=_VERTICALS, index="BITMAP"),
        "work_type":         FieldSpec("work_type", "string", ("=", "in"), enum=_WORK_TYPES, index="BITMAP"),
        "equipment_intensity": FieldSpec("equipment_intensity", "string", ("=", "in"),
                                         enum=_EQUIP_INTENSITY, index="BITMAP"),
        # PSC = the product/service the contract BUYS (distinct from NAICS, the vendor's
        # industry). psc_category is the leading PSC char ('V' = Transportation/Travel/
        # Relocation); psc_code is the full code. Prime-only (NULL on subawards).
        "psc_category":      FieldSpec("psc_category", "string", ("=", "in"), index="BITMAP"),
        "psc_code":          FieldSpec("psc_code", "string", ("=", "in"), index="BTREE"),
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
        # Contracting officer's small/large determination (prime-only — NULL on subawards).
        "business_size":     FieldSpec("business_size", "string", ("=", "in"),
                                       enum=_BUSINESS_SIZE, index="BITMAP"),
        "winner_type":       FieldSpec("winner_type", "string", ("=", "in"),
                                       enum=("prime_recipient", "subawardee"), index="BITMAP"),
        # Contract currently within its period of performance (pop_end >= today, build-time);
        # prime-only — NULL on subawards, so is_active=true excludes them honestly.
        # FPDS action type of this action; is_option_exercise = the 'EXERCISE AN OPTION' event —
        # the government committing the next work tranche on an existing contract (a mobilization-
        # capital trigger). award_amount on such a row IS the mobilization $ (the aggregate measure).
        "action_type":       FieldSpec("action_type", "string", ("=", "in"), index="BITMAP"),
        "is_option_exercise": FieldSpec("is_option_exercise", "bool", ("=",), index="BITMAP"),
        "is_active":         FieldSpec("is_active", "bool", ("=",), index="BITMAP"),
    },
    synonyms={
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
        # PSC category 'V' = Transportation/Travel/Relocation services — the freight/logistics
        # SERVICE buys a NAICS-only (48/49) filter misses (often coded under a non-48/49 NAICS).
        "transportation services": {"field": "psc_category", "op": "=", "value": "V"},
        "freight services":        {"field": "psc_category", "op": "=", "value": "V"},
        "psc v":                   {"field": "psc_category", "op": "=", "value": "V"},
        "small business":          {"field": "business_size", "op": "=", "value": "SMALL BUSINESS"},
        # Option-exercise = the mobilization-capital event (govt committing the next work tranche).
        "option exercise":         {"field": "is_option_exercise", "op": "=", "value": True},
        "option exercises":        {"field": "is_option_exercise", "op": "=", "value": True},
        "exercised option":        {"field": "is_option_exercise", "op": "=", "value": True},
        "options exercised":       {"field": "is_option_exercise", "op": "=", "value": True},
        # ── GTM label-axis lexicon (awards.v9). Plain-English → vertical / work_type /
        # equipment_intensity. Targets are byte-exact enum values. Head-coverage only (~80% of
        # both-codes $); bare "construction" stays on naics2 above (broad recall, no regression).
        # Ambiguous bare tokens (security, engineering, manufacturing, logistics, defense) are
        # deliberately NOT mapped — only disambiguated phrasings, so a clause is never a coin-flip.
        # vertical ──
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
        # "AEC" is the industry-standard umbrella for the built-environment cluster.
        "aec":                     {"field": "vertical", "op": "in",
                                    "value": ["Engineering & Architecture", "Construction"]},
        # work_type (make-vs-resell / mobilization-capital signal) ──
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
        # equipment_intensity (financing-need proxy; 'medium' has no natural phrase) ──
        "equipment-heavy":         {"field": "equipment_intensity", "op": "=", "value": "high"},
        "equipment-intensive":     {"field": "equipment_intensity", "op": "=", "value": "high"},
        "capital-intensive":       {"field": "equipment_intensity", "op": "=", "value": "high"},
        "asset-heavy":             {"field": "equipment_intensity", "op": "=", "value": "high"},
        "asset-light":             {"field": "equipment_intensity", "op": "=", "value": "low"},
        "labor-only":              {"field": "equipment_intensity", "op": "=", "value": "low"},
    },
    # Aggregate over award_amount, grouped by any indexed dim (or the 'winner'/'size_band'
    # pseudo-dims). The window stays query-driven: the SAME days_since_action filter the row
    # path uses scopes the aggregate — no hardcoded lookback. The serving table spans ~2y.
    aggregate=AggregateSpec(
        measure="award_amount",
        dims={
            "fiscal_year": "fiscal_year",   # group-by fiscal_year → the YoY spend trend
            "naics2": "naics2", "naics_code": "naics_code",
            "psc_category": "psc_category", "psc_code": "psc_code",
            "awarding_agency": "awarding_agency", "awarding_sub_agency": "awarding_sub_agency",
            "state": "state", "pop_state": "pop_state",
            "set_aside": "set_aside", "business_size": "business_size", "winner_type": "winner_type",
            "action_type": "action_type",   # group-by the action mix (new award / exercise / funding / mod)
            # GTM-attribute breakdowns: $ by industry vertical / work_type / equipment_intensity.
            "vertical": "vertical", "work_type": "work_type", "equipment_intensity": "equipment_intensity",
        },
        winner_key=("winner_uei", "winner_name"),
        size_band_edges=(25_000.0, 250_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0),
    ),
)


# FPDS award/IDV flag (verified live on the active-awards serving table; NULL on the ~unfilled
# tail, which an enum filter excludes honestly). _BUSINESS_SIZE is defined above (shared w/ awards).
_AWARD_OR_IDV = ("AWARD", "IDV")

ACTIVE = Decoder(
    dataset_key="active",
    version="active.v2",   # v1→v2: expose the GTM-attribute label axes vertical/work_type/
                           # equipment_intensity (filter fields + aggregate dims, all BITMAP) and
                           # carry what_was_done as a self-describing DISPLAY property — mirrors the
                           # awards.v9 change (PR #715 vertical labels, #720 what_was_done gloss) onto
                           # the FORWARD recompete dataset so "aerospace contracts up for recompete" /
                           # "IT vertical expiring this quarter" route on vertical. Head-coverage only
                           # (~78% of recompete $ but ~38% of rows — the unlabeled tail is small-dollar);
                           # unlabeled rows surface in 'not applied', never silently filtered.
    geometry=("longitude", "latitude"),
    properties=("award_id", "winner_uei", "winner_name", "current_value", "potential_value",
                "obligated", "pop_current_end", "pop_potential_end", "has_option_tail",
                "naics2", "naics_code", "psc_category", "psc_code", "state", "pop_state",
                "awarding_agency", "awarding_sub_agency", "set_aside", "business_size",
                "award_or_idv_flag",
                # GTM-attribute label axes + the free-text gloss. what_was_done is DISPLAY-only —
                # NOT a filter field, NOT indexed (it rides here so the feature self-describes).
                "vertical", "work_type", "equipment_intensity", "what_was_done"),
    fields={
        # THE forward axis — recompete radar. days_until_expiry <= N → pop_current_end in the next
        # N days (today..today+N); >= N → at least N days of runway left. Award-grain (1 row/award),
        # so a count/sum is an honest contract count, not an action count. Query-driven horizon.
        "days_until_expiry": FieldSpec("pop_current_end", "days_ahead", ("<=", ">=", "between"), index="BTREE"),
        "current_value":     FieldSpec("current_value", "float", (">=", "<=", "between"), index="BTREE"),
        "potential_value":   FieldSpec("potential_value", "float", (">=", "<=", "between"), index="BTREE"),
        "obligated":         FieldSpec("obligated", "float", (">=", "<=", "between"), index="BTREE"),
        "naics2":            FieldSpec("naics2", "string", ("=", "in"), index="BITMAP"),
        "naics_code":        FieldSpec("naics_code", "string", ("=", "in"), index="BTREE"),
        # ── GTM-attribute axes (PR #715/#720), mirrored from awards.v9 onto the recompete dataset:
        # the award's (naics, psc) pair classified into a rich VERTICAL (24-name taxonomy, distinct
        # from the raw naics2 sector), a WORK_TYPE (make vs resell vs build), and an
        # EQUIPMENT_INTENSITY band (financing proxy). All three BITMAP, head-coverage only; peer
        # filters that AND with naics2/psc. what_was_done is a DISPLAY property (see properties). ──
        "vertical":          FieldSpec("vertical", "string", ("=", "in"), enum=_VERTICALS, index="BITMAP"),
        "work_type":         FieldSpec("work_type", "string", ("=", "in"), enum=_WORK_TYPES, index="BITMAP"),
        "equipment_intensity": FieldSpec("equipment_intensity", "string", ("=", "in"),
                                         enum=_EQUIP_INTENSITY, index="BITMAP"),
        "psc_category":      FieldSpec("psc_category", "string", ("=", "in"), index="BITMAP"),
        "psc_code":          FieldSpec("psc_code", "string", ("=", "in"), index="BTREE"),
        "state":             FieldSpec("state", "string", ("=", "in"), index="BITMAP"),
        "pop_state":         FieldSpec("pop_state", "string", ("=", "in"), index="BITMAP"),
        "awarding_agency":   FieldSpec("awarding_agency", "string", ("=", "in"), index="BITMAP"),
        "awarding_sub_agency": FieldSpec("awarding_sub_agency", "string", ("=", "in"), index="BTREE"),
        "set_aside":         FieldSpec("set_aside", "string", ("=", "in"), enum=_SET_ASIDE_CODES, index="BITMAP"),
        "business_size":     FieldSpec("business_size", "string", ("=", "in"), enum=_BUSINESS_SIZE, index="BITMAP"),
        "has_option_tail":   FieldSpec("has_option_tail", "bool", ("=",), index="BITMAP"),
        "award_or_idv_flag": FieldSpec("award_or_idv_flag", "string", ("=", "in"), enum=_AWARD_OR_IDV, index="BITMAP"),
    },
    synonyms={
        "construction": {"field": "naics2", "op": "=", "value": "23"},
        "transportation services": {"field": "psc_category", "op": "=", "value": "V"},
        "freight services": {"field": "psc_category", "op": "=", "value": "V"},
        # Recompete radar phrasings — the forward window the dataset exists for.
        "expiring soon": {"field": "days_until_expiry", "op": "<=", "value": 90},
        "expiring this quarter": {"field": "days_until_expiry", "op": "<=", "value": 90},
        "recompete": {"field": "days_until_expiry", "op": "<=", "value": 180},
        "up for recompete": {"field": "days_until_expiry", "op": "<=", "value": 180},
        "small business": {"field": "business_size", "op": "=", "value": "SMALL BUSINESS"},
        "8(a)": {"field": "set_aside", "op": "in", "value": ["8A", "8AN"]},
        "sdvosb": {"field": "set_aside", "op": "in", "value": ["SDVOSBC", "SDVOSBS"]},
        "hubzone": {"field": "set_aside", "op": "in", "value": ["HZC", "HZS"]},
        "woman-owned": {"field": "set_aside", "op": "in", "value": ["WOSB", "WOSBSS", "EDWOSB", "EDWOSBSS"]},
        "dod": {"field": "awarding_agency", "op": "in",
                "value": ["Department of Defense", "Department of Defense (DOD)"]},
        # ── GTM label-axis lexicon (active.v2). Plain-English → vertical / work_type /
        # equipment_intensity. Byte-identical to the awards decoder + the edge mirror. Targets are
        # byte-exact enum values. Head-coverage only (~78% of recompete $, ~38% of rows); bare
        # "construction" stays on naics2 above (broad recall, no regression). Ambiguous bare tokens
        # (security, engineering, manufacturing, logistics, defense) are deliberately NOT mapped —
        # only disambiguated phrasings, so a clause is never a coin-flip.
        # vertical ──
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
        # "AEC" is the industry-standard umbrella for the built-environment cluster.
        "aec":                     {"field": "vertical", "op": "in",
                                    "value": ["Engineering & Architecture", "Construction"]},
        # work_type (make-vs-resell / mobilization-capital signal) ──
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
        # equipment_intensity (financing-need proxy; 'medium' has no natural phrase) ──
        "equipment-heavy":         {"field": "equipment_intensity", "op": "=", "value": "high"},
        "equipment-intensive":     {"field": "equipment_intensity", "op": "=", "value": "high"},
        "capital-intensive":       {"field": "equipment_intensity", "op": "=", "value": "high"},
        "asset-heavy":             {"field": "equipment_intensity", "op": "=", "value": "high"},
        "asset-light":             {"field": "equipment_intensity", "op": "=", "value": "low"},
        "labor-only":              {"field": "equipment_intensity", "op": "=", "value": "low"},
    },
    aggregate=AggregateSpec(
        measure="current_value",
        dims={
            "naics2": "naics2", "naics_code": "naics_code",
            "psc_category": "psc_category", "psc_code": "psc_code",
            "awarding_agency": "awarding_agency", "awarding_sub_agency": "awarding_sub_agency",
            "state": "state", "pop_state": "pop_state",
            "set_aside": "set_aside", "business_size": "business_size",
            # GTM-attribute breakdowns: $ by industry vertical / work_type / equipment_intensity.
            "vertical": "vertical", "work_type": "work_type", "equipment_intensity": "equipment_intensity",
        },
        winner_key=("winner_uei", "winner_name"),
        size_band_edges=(25_000.0, 250_000.0, 1_000_000.0, 10_000_000.0, 100_000_000.0),
    ),
)


DECODERS: dict[str, Decoder] = {"winners": WINNERS, "company": COMPANY, "awards": AWARDS, "active": ACTIVE}
