"""USAspending FPDS CANONICAL transaction table (LOCAL CLI) — typed v2 SoR reconciliation.

Reconciles the TWO FPDS transaction feeds into ONE typed, PK-grained read model
(`s3://data-sink/active/usaspending_fpds_canonical_txn/`, Lance v2.1, 392 typed columns — the OBT):

    BULK   s3://data-sink/active/usaspending/transaction_search_fpds/   (~107.25M, 378 typed rpt.* cols)
    FRESH  s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/ (~1.99M, 297 all-VARCHAR)

The MONTHLY / archive CSV feed (physical usaspending_archive_*_fpds) is OUT OF SCOPE in this build —
its re-integration is owned by a parallel agent. The 12 monthly-unique enrichment cols are typed-NULL
placeholders (feed_expr=None AND bulk_expr=None) reserved for that re-add; the schema stays 392-wide.

CANONICAL VOCABULARY = the FPDS bulk_download/awards names (FRESH carries them verbatim); BULK is
crosswalked into that vocabulary via the rpt.* map. BULK-only enrichment columns keep their rpt.*
names verbatim (the OBT carries all 378 BULK-dictionary columns).

MERGE (2-source BULK+FRESH reconciliation):
  • s()/kbulk() sentinel macros applied IDENTICALLY on both sources — whole-string ''/'-NONE-' → NULL.
  • fresh_latest / bulk_latest: EACH source collapsed to latest-per-key (deterministic tiebreaker).
    bulk_latest is ONE per-key collapse over FULL BULK (107M scanned ONCE) and is BOTH a core competitor
    AND the SOLE pg enrichment source.
  • CORE: per-key argmax(last_modified_date) over the union of the two collapsed cores, executed as ONE
    2-way row_number() window (source_rank FRESH<BULK) — equal-mtime tie → FRESH wins.
  • Enrichment: pg-only fill from bulk_latest (plain b.<col> for every enrich col). The 12 monthly-unique
    cols resolve to CAST(NULL AS <type>) (bulk_expr None) — typed-NULL placeholders. recipient_uei is
    CORE (argmax-resolved), NOT enrichment.
  • canonical_source: derived ONCE as the winning core row's src tag (fresh|bulk) — the true per-key
    winner, never a partition literal.
  • Fail-closed PK-uniqueness gate raises BEFORE publish on any dup (structural: one survivor per key).

DISCIPLINES (d.8 / fleet rules):
  • module-top os.environ.setdefault("LANCE_BYPASS_SPILLING","true") BEFORE any import lance.
  • NO direct-R2 write of the table (Giants 400 InvalidPart) — LOCAL Lance write → boto3 uniform-part
    publish. data_storage_version="2.1", max_rows_per_file=1048576 (valid only on the boto3 path).
  • built_at = ONE Python naive-UTC literal injected into both projections (NOT now()).
  • last_modified_date parsed via replace(...,'+00','')+TRY_CAST (NO strptime hard-abort).
  • NO auto-retries in pipeline logic; overwrite idempotency.
  • --since pushes action_date>= into the TWO DATA scanners (BULK date32; FRESH lexical ISO-10 string).

    # SAMPLE (on-box, 48GiB/3GB-free — SAMPLE ONLY, never prod):
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'boto3>=1.34' --with 'psycopg[binary]>=3.2' \
      python3 -m pipelines.usaspending.usaspending_fpds_canonical \
        build --since 2025-10-01 \
        --target-uri s3://data-sink/active/_sample/usaspending_fpds_canonical_txn_sample/

    # FULL (proper compute, >=96GiB box): FPDS_CANONICAL_DUCKDB_MEM=96GB FPDS_CANONICAL_DUCKDB_THREADS=8
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'boto3>=1.34' --with 'psycopg[binary]>=3.2' \
      python3 -m pipelines.usaspending.usaspending_fpds_canonical build      # then: index ; then: verify

    python3 -m pipelines.usaspending.usaspending_fpds_canonical init_ops
    python3 -m pipelines.usaspending.usaspending_fpds_canonical index  [--target-uri URI]
    python3 -m pipelines.usaspending.usaspending_fpds_canonical verify [--target-uri URI]
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import sys
from pathlib import Path

# In-RAM scalar-index sort — the small DataFusion external-merge pool OOMs ("ExternalSorterMerge")
# on a ~107M-row BTREE build. Set BEFORE any lance call (ARCHITECTURE.md fleet rule).
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

try:  # Modal is the production build substrate; the local CLI stays the dev/smoke path.
    import modal
except ImportError:
    modal = None

BUCKET = "data-sink"
ACTIVE = "s3://data-sink/active"

BULK_URI = f"{ACTIVE}/usaspending/transaction_search_fpds/"
FRESH_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_txn/"
ARCHIVE_FULL_URI = f"{ACTIVE}/usaspending_archive_full_fpds/"
ARCHIVE_DELTA_URI = f"{ACTIVE}/usaspending_archive_delta_fpds/"
CANONICAL_URI = f"{ACTIVE}/usaspending_fpds_canonical_txn/"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576          # valid ONLY on the boto3-publish path (uniform parts)
MAX_BYTES_PER_FILE = 90 * 1024**3

SCRATCH = os.environ.get("FPDS_CANONICAL_SCRATCH", "/tmp/fpds_canonical_stage")
DUCK_MEM = os.environ.get("FPDS_CANONICAL_DUCKDB_MEM", "8GB")
DUCK_TMP = os.environ.get("FPDS_CANONICAL_DUCKDB_TEMP_DIR", "/tmp/fpds_canonical_duckdb")
DUCK_THREADS = int(os.environ.get("FPDS_CANONICAL_DUCKDB_THREADS", "4"))

FEED = "usaspending_fpds_canonical_txn"
OPS_SQL_FILE = "ops_usaspending_fpds_canonical_runs.sql"
OPS_TABLE = "ops.usaspending_fpds_canonical_runs"


# =========================================================================================== #
# THE COLUMN CONTRACT — the SINGLE source of truth.
# Every projection, the b_wins CASE block, the enrichment REPLACE block, the final column order,
# and the index lists are PROGRAM-GENERATED from this structure. A transcription error in 78x3
# hand-written columns is the top risk → generate, do not type them out.
#
# Each entry:
#   canonical : output column name (= FRESH/awards vocabulary for keys/core; rpt.* verbatim for enrich)
#   duck_type : DuckDB target type (DATE→Arrow date32, TIMESTAMP→naive timestamp[us], DOUBLE, BIGINT, VARCHAR)
#   group     : 'key' | 'core' | 'enrich' | 'prov'
#   bulk_expr : BULK projection expr (rpt.* crosswalk). None ⇒ typed NULL placeholder on the BULK leg.
#   feed_expr : FRESH/archive projection expr (canonical-vocabulary). None ⇒ typed NULL placeholder
#               (enrichment cols have no FRESH/archive source).
# Macros (defined once, used identically everywhere — see _MACROS):
#   s(x)              : nullif(nullif(trim(x), ''), '-NONE-')   generic VARCHAR sentinel-null
#   kbulk(det, txn)   : s(COALESCE(s(det), s(txn)))             BULK txn-key, sentinel-normalized like s()
# Typed-cast idiom (VARCHAR source → typed): TRY_CAST(s(x) AS <T>).
# last_modified_date (BLOCKER): NO strptime — TRY_CAST(replace(s(x),'+00','') AS TIMESTAMP).
# =========================================================================================== #
COLUMN_SPEC: list[dict] = [
    # ---- (a) PK / keys ----
    {"canonical": "contract_transaction_unique_key", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "kbulk(detached_award_proc_unique, transaction_unique_id)",
     "feed_expr": "s(contract_transaction_unique_key)"},
    {"canonical": "contract_award_unique_key", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(generated_unique_award_id)",
     "feed_expr": "s(contract_award_unique_key)"},

    # ---- (b) volatile-core ----
    {"canonical": "action_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "action_date", "feed_expr": "TRY_CAST(s(action_date) AS DATE)"},
    {"canonical": "last_modified_date", "duck_type": "TIMESTAMP", "group": "core",
     "bulk_expr": "last_modified_date",
     "feed_expr": "TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP)"},
    {"canonical": "period_of_performance_start_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "period_of_performance_start_date",
     "feed_expr": "TRY_CAST(s(period_of_performance_start_date) AS DATE)"},
    {"canonical": "period_of_performance_current_end_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "period_of_performance_current_end_date",
     "feed_expr": "TRY_CAST(s(period_of_performance_current_end_date) AS DATE)"},
    {"canonical": "federal_action_obligation", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "federal_action_obligation",
     "feed_expr": "TRY_CAST(s(federal_action_obligation) AS DOUBLE)"},
    {"canonical": "base_and_all_options_value", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "TRY_CAST(s(base_and_all_options_value) AS DOUBLE)",
     "feed_expr": "TRY_CAST(s(base_and_all_options_value) AS DOUBLE)"},
    {"canonical": "current_total_value_of_award", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "TRY_CAST(s(current_total_value_award) AS DOUBLE)",
     "feed_expr": "TRY_CAST(s(current_total_value_of_award) AS DOUBLE)"},
    {"canonical": "modification_number", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(modification_number)", "feed_expr": "s(modification_number)"},
    {"canonical": "award_id_piid", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(piid)", "feed_expr": "s(award_id_piid)"},
    {"canonical": "parent_award_id_piid", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(parent_award_id)", "feed_expr": "s(parent_award_id_piid)"},
    {"canonical": "recipient_uei", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(recipient_uei)", "feed_expr": "s(recipient_uei)"},
    {"canonical": "recipient_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(recipient_name)", "feed_expr": "s(recipient_name)"},
    {"canonical": "cage_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(cage_code)", "feed_expr": "s(cage_code)"},
    {"canonical": "recipient_address_line_1", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(legal_entity_address_line1)", "feed_expr": "s(recipient_address_line_1)"},
    {"canonical": "recipient_city_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(recipient_location_city_name)", "feed_expr": "s(recipient_city_name)"},
    {"canonical": "recipient_county_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(recipient_location_county_name)", "feed_expr": "s(recipient_county_name)"},
    {"canonical": "recipient_state_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(recipient_location_state_code)", "feed_expr": "s(recipient_state_code)"},
    {"canonical": "recipient_zip_4_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": None, "feed_expr": "s(recipient_zip_4_code)"},  # BULK has no zip4 (carries zip5 in enrich)
    {"canonical": "recipient_country_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(recipient_location_country_code)", "feed_expr": "s(recipient_country_code)"},
    {"canonical": "naics_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(naics_code)", "feed_expr": "s(naics_code)"},
    {"canonical": "naics_description", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(naics_description)", "feed_expr": "s(naics_description)"},
    {"canonical": "product_or_service_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(product_or_service_code)", "feed_expr": "s(product_or_service_code)"},
    {"canonical": "product_or_service_code_description", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(product_or_service_description)", "feed_expr": "s(product_or_service_code_description)"},
    {"canonical": "type_of_set_aside_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(type_set_aside)", "feed_expr": "s(type_of_set_aside_code)"},
    {"canonical": "extent_competed", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(extent_competed)", "feed_expr": "s(extent_competed)"},
    {"canonical": "type_of_contract_pricing_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(type_of_contract_pricing)", "feed_expr": "s(type_of_contract_pricing_code)"},
    {"canonical": "award_type_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(contract_award_type)", "feed_expr": "s(award_type_code)"},
    {"canonical": "idv_type_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(idv_type)", "feed_expr": "s(idv_type_code)"},
    {"canonical": "subcontracting_plan", "duck_type": "VARCHAR", "group": "core",
     # FPDS subcontracting-plan CODE (A/B/C/D/E/F/G/H). BULK exposes the code as subcontracting_plan;
     # FRESH + archive expose it as subcontracting_plan_code (their `subcontracting_plan` is the
     # description, not used). Codes C/D/E/F/G/H ⇒ a plan is in place; A/B ⇒ none; NULL ⇒ not populated
     # (~35% of txns). has_subcontracting_plan derives downstream: subcontracting_plan IN
     # ('C','D','E','F','G','H') — the raw code is carried on the spine; the boolean lives in serving.
     "bulk_expr": "s(subcontracting_plan)", "feed_expr": "s(subcontracting_plan_code)"},
    {"canonical": "construction_wage_rate_requirements_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(construction_wage_rate_req)",  # NON-1:1 rename (truncated pg col name)
     "feed_expr": "s(construction_wage_rate_requirements_code)"},
    {"canonical": "labor_standards_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(labor_standards)", "feed_expr": "s(labor_standards_code)"},
    {"canonical": "awarding_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_agency_code)", "feed_expr": "s(awarding_agency_code)"},
    {"canonical": "awarding_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_toptier_agency_name)", "feed_expr": "s(awarding_agency_name)"},
    {"canonical": "awarding_sub_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_sub_tier_agency_c)", "feed_expr": "s(awarding_sub_agency_code)"},
    {"canonical": "awarding_sub_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_subtier_agency_name)", "feed_expr": "s(awarding_sub_agency_name)"},
    {"canonical": "funding_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_agency_code)", "feed_expr": "s(funding_agency_code)"},
    {"canonical": "funding_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_toptier_agency_name)", "feed_expr": "s(funding_agency_name)"},
    {"canonical": "primary_place_of_performance_state_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(pop_state_code)", "feed_expr": "s(primary_place_of_performance_state_code)"},
    {"canonical": "primary_place_of_performance_country_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(pop_country_code)", "feed_expr": "s(primary_place_of_performance_country_code)"},
    {"canonical": "primary_place_of_performance_zip_4", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(pop_zip5)",  # BULK carries zip5 only — documented lossy best-effort
     "feed_expr": "s(primary_place_of_performance_zip_4)"},
    {"canonical": "contracting_officers_determination_of_business_size", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(contracting_officers_deter)",
     "feed_expr": "s(contracting_officers_determination_of_business_size)"},
    {"canonical": "action_type_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(action_type)", "feed_expr": "s(action_type_code)"},
    {"canonical": "transaction_description", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(transaction_description)", "feed_expr": "s(transaction_description)"},
    {"canonical": "solicitation_identifier", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(solicitation_identifier)", "feed_expr": "s(solicitation_identifier)"},
    {"canonical": "action_date_fiscal_year", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "fiscal_year", "feed_expr": "TRY_CAST(s(action_date_fiscal_year) AS BIGINT)"},

    # ---- (c) BULK-only enrichment (rpt.* names verbatim; feed_expr None ⇒ typed NULL on FRESH/archive) ----
    {"canonical": "recipient_hash", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(recipient_hash)", "feed_expr": None},
    {"canonical": "recipient_levels", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(recipient_levels)", "feed_expr": None},
    {"canonical": "parent_uei", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(parent_uei)", "feed_expr": None},
    {"canonical": "parent_recipient_hash", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(parent_recipient_hash)", "feed_expr": None},
    {"canonical": "parent_recipient_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(parent_recipient_name)", "feed_expr": None},
    {"canonical": "business_categories", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(business_categories)", "feed_expr": None},
    {"canonical": "business_types", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(business_types)", "feed_expr": None},
    {"canonical": "federal_accounts", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(federal_accounts)", "feed_expr": None},
    {"canonical": "treasury_account_identifiers", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": "treasury_account_identifiers", "feed_expr": None},
    {"canonical": "tas_paths", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(tas_paths)", "feed_expr": None},
    {"canonical": "disaster_emergency_fund_codes", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(disaster_emergency_fund_codes)", "feed_expr": None},
    {"canonical": "program_activities", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(program_activities)", "feed_expr": None},
    {"canonical": "cfda_number", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(cfda_number)", "feed_expr": None},
    {"canonical": "awarding_toptier_agency_abbreviation", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(awarding_toptier_agency_abbreviation)", "feed_expr": None},
    {"canonical": "awarding_subtier_agency_abbreviation", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(awarding_subtier_agency_abbreviation)", "feed_expr": None},
    {"canonical": "awarding_agency_id", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": "awarding_agency_id", "feed_expr": None},
    {"canonical": "funding_agency_id", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": "funding_agency_id", "feed_expr": None},
    {"canonical": "award_id", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": "award_id", "feed_expr": None},
    {"canonical": "transaction_id", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": "transaction_id", "feed_expr": None},
    {"canonical": "recipient_location_zip5", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(recipient_location_zip5)", "feed_expr": None},
    {"canonical": "recipient_location_county_fips", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(recipient_location_county_fips)", "feed_expr": None},
    {"canonical": "recipient_location_congressional_code", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(recipient_location_congressional_code)", "feed_expr": None},
    {"canonical": "pop_county_fips", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(pop_county_fips)", "feed_expr": None},
    {"canonical": "pop_congressional_code", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(pop_congressional_code)", "feed_expr": None},
    {"canonical": "award_category", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(award_category)", "feed_expr": None},
    {"canonical": "award_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": "award_amount", "feed_expr": None},
    {"canonical": "total_funding_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": "total_funding_amount", "feed_expr": None},

    # ---- (c2) MONTHLY-unique enrichment — TYPED-NULL PLACEHOLDERS in this 2-source (BULK+FRESH) build.
    #   pg/BULK LACKS all 12 (bulk_expr None) AND monthly is OUT OF SCOPE here (feed_expr None) → each
    #   is CAST(NULL AS <type>) on BOTH legs, 100% NULL in the live dataset. These 12 rows ARE the
    #   coordination contract for the parallel MONTHLY agent: it flips feed_expr back on (or points them
    #   at the renamed monthly upstream) and re-adds the monthly collapse + COALESCE leg — the schema
    #   stays 392-wide throughout. Do NOT populate here. See the module MERGE header + Phase B of
    #   docs/reference/FPDS_CANONICAL_OBT_EXECUTION_PLAN.md. ----
    {"canonical": "treasury_accounts_funding_this_award", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "federal_accounts_funding_this_award", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_1_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_2_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_3_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_4_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_5_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_1_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_2_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_3_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_4_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},
    {"canonical": "highly_compensated_officer_5_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": None},

    # ---- (c2) FPDS spine expansion — tightened adds (2026-07-02; see FPDS_CANONICAL_FIELD_DICTIONARY.md) ----
    {"canonical": "women_owned_small_business", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "upper(substr(CAST(women_owned_small_business AS VARCHAR),1,1))",
     "feed_expr": "upper(substr(CAST(women_owned_small_business AS VARCHAR),1,1))"},
    {"canonical": "service_disabled_veteran_owned_business", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "upper(substr(CAST(service_disabled_veteran_o AS VARCHAR),1,1))",
     "feed_expr": "upper(substr(CAST(service_disabled_veteran_owned_business AS VARCHAR),1,1))"},
    {"canonical": "historically_underutilized_business_zone_hubzone_firm", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "upper(substr(CAST(historically_underutilized AS VARCHAR),1,1))",
     "feed_expr": "upper(substr(CAST(historically_underutilized_business_zone_hubzone_firm AS VARCHAR),1,1))"},
    {"canonical": "c8a_program_participant", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "upper(substr(CAST(c8a_program_participant AS VARCHAR),1,1))",
     "feed_expr": "upper(substr(CAST(c8a_program_participant AS VARCHAR),1,1))"},
    {"canonical": "solicitation_procedures", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(solicitation_procedures)", "feed_expr": "s(solicitation_procedures)"},
    {"canonical": "other_than_full_and_open_competition_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(other_than_full_and_open_c)", "feed_expr": "s(other_than_full_and_open_competition_code)"},
    {"canonical": "fair_opportunity_limited_sources_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(fair_opportunity_limited_s)", "feed_expr": "s(fair_opportunity_limited_sources_code)"},
    {"canonical": "commercial_item_acquisition_procedures_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(commercial_item_acquisitio)", "feed_expr": "s(commercial_item_acquisition_procedures_code)"},
    {"canonical": "multiple_or_single_award_idv_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(multiple_or_single_award_i)", "feed_expr": "s(multiple_or_single_award_idv_code)"},
    {"canonical": "parent_award_agency_id", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(referenced_idv_agency_iden)", "feed_expr": "s(parent_award_agency_id)"},
    {"canonical": "parent_award_type_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(referenced_idv_type)", "feed_expr": "s(parent_award_type_code)"},
    {"canonical": "parent_award_modification_number", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(referenced_idv_modificatio)", "feed_expr": "s(parent_award_modification_number)"},
    {"canonical": "major_program", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(major_program)", "feed_expr": "s(major_program)"},
    {"canonical": "program_acronym", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(program_acronym)", "feed_expr": "s(program_acronym)"},
    {"canonical": "contract_bundling", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(contract_bundling)", "feed_expr": "s(contract_bundling)"},
    {"canonical": "consolidated_contract", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(consolidated_contract)", "feed_expr": "s(consolidated_contract)"},
    {"canonical": "performance_based_service_acquisition_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(performance_based_service)", "feed_expr": "s(performance_based_service_acquisition_code)"},
    {"canonical": "undefinitized_action_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(undefinitized_action)", "feed_expr": "s(undefinitized_action_code)"},
    {"canonical": "multi_year_contract", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(multi_year_contract)", "feed_expr": "s(multi_year_contract)"},
    {"canonical": "contract_financing", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(contract_financing)", "feed_expr": "s(contract_financing)"},
    {"canonical": "cost_or_pricing_data", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(cost_or_pricing_data)", "feed_expr": "s(cost_or_pricing_data)"},
    {"canonical": "dod_claimant_program_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(dod_claimant_program_code)", "feed_expr": "s(dod_claimant_program_code)"},
    {"canonical": "inherently_governmental_functions", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(inherently_government_func)", "feed_expr": "s(inherently_governmental_functions)"},
    {"canonical": "purchase_card_as_payment_method_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(purchase_card_as_payment_m)", "feed_expr": "s(purchase_card_as_payment_method_code)"},
    {"canonical": "clinger_cohen_act_planning_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(clinger_cohen_act_planning)", "feed_expr": "s(clinger_cohen_act_planning_code)"},
    {"canonical": "national_interest_action_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(national_interest_action)", "feed_expr": "s(national_interest_action_code)"},
    {"canonical": "domestic_or_foreign_entity_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(domestic_or_foreign_entity)", "feed_expr": "s(domestic_or_foreign_entity_code)"},
    {"canonical": "price_evaluation_adjustment_preference_percent_difference", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(price_evaluation_adjustmen)", "feed_expr": "s(price_evaluation_adjustment_preference_percent_difference)"},
    {"canonical": "place_of_performance_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_performance_code)", "feed_expr": None},
    {"canonical": "place_of_performance_scope", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_performance_scope)", "feed_expr": None},
    {"canonical": "place_of_performance_forei", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_performance_forei)", "feed_expr": None},
    {"canonical": "pop_city_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(pop_city_name)", "feed_expr": None},
    {"canonical": "funding_office_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_office_code)", "feed_expr": "s(funding_office_code)"},
    {"canonical": "funding_office_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_office_name)", "feed_expr": "s(funding_office_name)"},
    {"canonical": "funding_sub_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_sub_tier_agency_co)", "feed_expr": "s(funding_sub_agency_code)"},
    {"canonical": "funding_sub_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_subtier_agency_name)", "feed_expr": "s(funding_sub_agency_name)"},
    {"canonical": "transaction_number", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(transaction_number)", "feed_expr": "s(transaction_number)"},
    {"canonical": "ordering_period_end_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "TRY_CAST(s(ordering_period_end_date) AS DATE)", "feed_expr": "TRY_CAST(s(ordering_period_end_date) AS DATE)"},
    {"canonical": "solicitation_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "solicitation_date", "feed_expr": "TRY_CAST(s(solicitation_date) AS DATE)"},
    {"canonical": "total_dollars_obligated", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "TRY_CAST(s(total_obligated_amount) AS DOUBLE)", "feed_expr": "TRY_CAST(s(total_dollars_obligated) AS DOUBLE)"},
    {"canonical": "base_and_exercised_options_value", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "TRY_CAST(s(base_exercised_options_val) AS DOUBLE)", "feed_expr": "TRY_CAST(s(base_and_exercised_options_value) AS DOUBLE)"},
    {"canonical": "number_of_offers_received", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "TRY_CAST(s(number_of_offers_received) AS BIGINT)", "feed_expr": "TRY_CAST(s(number_of_offers_received) AS BIGINT)"},
    {"canonical": "number_of_actions", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "TRY_CAST(s(number_of_actions) AS BIGINT)", "feed_expr": "TRY_CAST(s(number_of_actions) AS BIGINT)"},
    # ── OBT expansion: 261 BULK-native "documented but not carried" columns ──────────
    # All group="enrich", feed_expr=None (BULK-only pg enrichment), native typing.
    # Generated deterministically from live BULK (378) − already-referenced (117) = 261.
    # See docs/reference/fpds_obt_261_additions.json (committed derivation artifact).
    {"canonical": "a_76_fair_act_action", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(a_76_fair_act_action)", "feed_expr": None},
    {"canonical": "a_76_fair_act_action_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(a_76_fair_act_action_desc)", "feed_expr": None},
    {"canonical": "action_type_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(action_type_description)", "feed_expr": None},
    {"canonical": "afa_generated_unique", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(afa_generated_unique)", "feed_expr": None},
    {"canonical": "agency_id", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(agency_id)", "feed_expr": None},
    {"canonical": "airport_authority", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "airport_authority", "feed_expr": None},
    {"canonical": "alaskan_native_owned_corpo", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "alaskan_native_owned_corpo", "feed_expr": None},
    {"canonical": "alaskan_native_servicing_i", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "alaskan_native_servicing_i", "feed_expr": None},
    {"canonical": "american_indian_owned_busi", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "american_indian_owned_busi", "feed_expr": None},
    {"canonical": "asian_pacific_american_own", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "asian_pacific_american_own", "feed_expr": None},
    {"canonical": "award_certified_date", "duck_type": "DATE", "group": "enrich", "bulk_expr": "award_certified_date", "feed_expr": None},
    {"canonical": "award_date_signed", "duck_type": "DATE", "group": "enrich", "bulk_expr": "award_date_signed", "feed_expr": None},
    {"canonical": "award_fiscal_year", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "award_fiscal_year", "feed_expr": None},
    {"canonical": "award_update_date", "duck_type": "TIMESTAMP", "group": "enrich", "bulk_expr": "award_update_date", "feed_expr": None},
    {"canonical": "awarding_office_code", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(awarding_office_code)", "feed_expr": None},
    {"canonical": "awarding_office_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(awarding_office_name)", "feed_expr": None},
    {"canonical": "awarding_subtier_agency_name_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(awarding_subtier_agency_name_raw)", "feed_expr": None},
    {"canonical": "awarding_toptier_agency_id", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "awarding_toptier_agency_id", "feed_expr": None},
    {"canonical": "awarding_toptier_agency_name_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(awarding_toptier_agency_name_raw)", "feed_expr": None},
    {"canonical": "black_american_owned_busin", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "black_american_owned_busin", "feed_expr": None},
    {"canonical": "business_funds_ind_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(business_funds_ind_desc)", "feed_expr": None},
    {"canonical": "business_funds_indicator", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(business_funds_indicator)", "feed_expr": None},
    {"canonical": "business_types_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(business_types_desc)", "feed_expr": None},
    {"canonical": "c1862_land_grant_college", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "c1862_land_grant_college", "feed_expr": None},
    {"canonical": "c1890_land_grant_college", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "c1890_land_grant_college", "feed_expr": None},
    {"canonical": "c1994_land_grant_college", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "c1994_land_grant_college", "feed_expr": None},
    {"canonical": "cfda_id", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "cfda_id", "feed_expr": None},
    {"canonical": "cfda_title", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(cfda_title)", "feed_expr": None},
    {"canonical": "city_local_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "city_local_government", "feed_expr": None},
    {"canonical": "clinger_cohen_act_pla_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(clinger_cohen_act_pla_desc)", "feed_expr": None},
    {"canonical": "commercial_item_acqui_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(commercial_item_acqui_desc)", "feed_expr": None},
    {"canonical": "commercial_item_test_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(commercial_item_test_desc)", "feed_expr": None},
    {"canonical": "commercial_item_test_progr", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(commercial_item_test_progr)", "feed_expr": None},
    {"canonical": "community_developed_corpor", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "community_developed_corpor", "feed_expr": None},
    {"canonical": "community_development_corp", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "community_development_corp", "feed_expr": None},
    {"canonical": "consolidated_contract_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(consolidated_contract_desc)", "feed_expr": None},
    {"canonical": "construction_wage_rat_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(construction_wage_rat_desc)", "feed_expr": None},
    {"canonical": "contingency_humanitar_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(contingency_humanitar_desc)", "feed_expr": None},
    {"canonical": "contingency_humanitarian_o", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(contingency_humanitarian_o)", "feed_expr": None},
    {"canonical": "contract_award_type_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(contract_award_type_desc)", "feed_expr": None},
    {"canonical": "contract_bundling_descrip", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(contract_bundling_descrip)", "feed_expr": None},
    {"canonical": "contract_financing_descrip", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(contract_financing_descrip)", "feed_expr": None},
    {"canonical": "contracting_officers_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(contracting_officers_desc)", "feed_expr": None},
    {"canonical": "contracts", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "contracts", "feed_expr": None},
    {"canonical": "corporate_entity_not_tax_e", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "corporate_entity_not_tax_e", "feed_expr": None},
    {"canonical": "corporate_entity_tax_exemp", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "corporate_entity_tax_exemp", "feed_expr": None},
    {"canonical": "correction_delete_ind_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(correction_delete_ind_desc)", "feed_expr": None},
    {"canonical": "correction_delete_indicatr", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(correction_delete_indicatr)", "feed_expr": None},
    {"canonical": "cost_accounting_stand_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(cost_accounting_stand_desc)", "feed_expr": None},
    {"canonical": "cost_accounting_standards", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(cost_accounting_standards)", "feed_expr": None},
    {"canonical": "cost_or_pricing_data_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(cost_or_pricing_data_desc)", "feed_expr": None},
    {"canonical": "council_of_governments", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "council_of_governments", "feed_expr": None},
    {"canonical": "country_of_product_or_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(country_of_product_or_desc)", "feed_expr": None},
    {"canonical": "country_of_product_or_serv", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(country_of_product_or_serv)", "feed_expr": None},
    {"canonical": "county_local_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "county_local_government", "feed_expr": None},
    {"canonical": "create_date", "duck_type": "TIMESTAMP", "group": "enrich", "bulk_expr": "create_date", "feed_expr": None},
    {"canonical": "detached_award_procurement_id", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "detached_award_procurement_id", "feed_expr": None},
    {"canonical": "dod_claimant_prog_cod_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(dod_claimant_prog_cod_desc)", "feed_expr": None},
    {"canonical": "domestic_or_foreign_e_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(domestic_or_foreign_e_desc)", "feed_expr": None},
    {"canonical": "domestic_shelter", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "domestic_shelter", "feed_expr": None},
    {"canonical": "dot_certified_disadvantage", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "dot_certified_disadvantage", "feed_expr": None},
    {"canonical": "economically_disadvantaged", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "economically_disadvantaged", "feed_expr": None},
    {"canonical": "educational_institution", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "educational_institution", "feed_expr": None},
    {"canonical": "emerging_small_business", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "emerging_small_business", "feed_expr": None},
    {"canonical": "epa_designated_produc_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(epa_designated_produc_desc)", "feed_expr": None},
    {"canonical": "epa_designated_product", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(epa_designated_product)", "feed_expr": None},
    {"canonical": "etl_update_date", "duck_type": "TIMESTAMP", "group": "enrich", "bulk_expr": "etl_update_date", "feed_expr": None},
    {"canonical": "evaluated_preference", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(evaluated_preference)", "feed_expr": None},
    {"canonical": "evaluated_preference_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(evaluated_preference_desc)", "feed_expr": None},
    {"canonical": "extent_compete_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(extent_compete_description)", "feed_expr": None},
    {"canonical": "face_value_loan_guarantee", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "face_value_loan_guarantee", "feed_expr": None},
    {"canonical": "fain", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(fain)", "feed_expr": None},
    {"canonical": "fair_opportunity_limi_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(fair_opportunity_limi_desc)", "feed_expr": None},
    {"canonical": "fed_biz_opps", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(fed_biz_opps)", "feed_expr": None},
    {"canonical": "fed_biz_opps_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(fed_biz_opps_description)", "feed_expr": None},
    {"canonical": "federal_agency", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "federal_agency", "feed_expr": None},
    {"canonical": "federally_funded_research", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "federally_funded_research", "feed_expr": None},
    {"canonical": "fiscal_action_date", "duck_type": "DATE", "group": "enrich", "bulk_expr": "fiscal_action_date", "feed_expr": None},
    {"canonical": "for_profit_organization", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "for_profit_organization", "feed_expr": None},
    {"canonical": "foreign_funding", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(foreign_funding)", "feed_expr": None},
    {"canonical": "foreign_funding_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(foreign_funding_desc)", "feed_expr": None},
    {"canonical": "foreign_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "foreign_government", "feed_expr": None},
    {"canonical": "foreign_owned_and_located", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "foreign_owned_and_located", "feed_expr": None},
    {"canonical": "foundation", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "foundation", "feed_expr": None},
    {"canonical": "funding_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "funding_amount", "feed_expr": None},
    {"canonical": "funding_opportunity_goals", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(funding_opportunity_goals)", "feed_expr": None},
    {"canonical": "funding_opportunity_number", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(funding_opportunity_number)", "feed_expr": None},
    {"canonical": "funding_subtier_agency_abbreviation", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(funding_subtier_agency_abbreviation)", "feed_expr": None},
    {"canonical": "funding_subtier_agency_name_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(funding_subtier_agency_name_raw)", "feed_expr": None},
    {"canonical": "funding_toptier_agency_abbreviation", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(funding_toptier_agency_abbreviation)", "feed_expr": None},
    {"canonical": "funding_toptier_agency_id", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "funding_toptier_agency_id", "feed_expr": None},
    {"canonical": "funding_toptier_agency_name_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(funding_toptier_agency_name_raw)", "feed_expr": None},
    {"canonical": "generated_pragmatic_obligation", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "generated_pragmatic_obligation", "feed_expr": None},
    {"canonical": "government_furnished_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(government_furnished_desc)", "feed_expr": None},
    {"canonical": "government_furnished_prope", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(government_furnished_prope)", "feed_expr": None},
    {"canonical": "grants", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "grants", "feed_expr": None},
    {"canonical": "hispanic_american_owned_bu", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "hispanic_american_owned_bu", "feed_expr": None},
    {"canonical": "hispanic_servicing_institu", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "hispanic_servicing_institu", "feed_expr": None},
    {"canonical": "historically_black_college", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "historically_black_college", "feed_expr": None},
    {"canonical": "hospital_flag", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "hospital_flag", "feed_expr": None},
    {"canonical": "housing_authorities_public", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "housing_authorities_public", "feed_expr": None},
    {"canonical": "idv_type_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(idv_type_description)", "feed_expr": None},
    {"canonical": "indian_tribe_federally_rec", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "indian_tribe_federally_rec", "feed_expr": None},
    {"canonical": "indirect_federal_sharing", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "indirect_federal_sharing", "feed_expr": None},
    {"canonical": "information_technolog_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(information_technolog_desc)", "feed_expr": None},
    {"canonical": "information_technology_com", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(information_technology_com)", "feed_expr": None},
    {"canonical": "ingested_at", "duck_type": "TIMESTAMP", "group": "enrich", "bulk_expr": "CAST(ingested_at AS TIMESTAMP)", "feed_expr": None},
    {"canonical": "inherently_government_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(inherently_government_desc)", "feed_expr": None},
    {"canonical": "initial_report_date", "duck_type": "TIMESTAMP", "group": "enrich", "bulk_expr": "initial_report_date", "feed_expr": None},
    {"canonical": "inter_municipal_local_gove", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "inter_municipal_local_gove", "feed_expr": None},
    {"canonical": "interagency_contract_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(interagency_contract_desc)", "feed_expr": None},
    {"canonical": "interagency_contracting_au", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(interagency_contracting_au)", "feed_expr": None},
    {"canonical": "international_organization", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "international_organization", "feed_expr": None},
    {"canonical": "interstate_entity", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "interstate_entity", "feed_expr": None},
    {"canonical": "is_fpds", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "is_fpds", "feed_expr": None},
    {"canonical": "joint_venture_economically", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "joint_venture_economically", "feed_expr": None},
    {"canonical": "joint_venture_women_owned", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "joint_venture_women_owned", "feed_expr": None},
    {"canonical": "labor_standards_descrip", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(labor_standards_descrip)", "feed_expr": None},
    {"canonical": "labor_surplus_area_firm", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "labor_surplus_area_firm", "feed_expr": None},
    {"canonical": "legal_entity_address_line2", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_address_line2)", "feed_expr": None},
    {"canonical": "legal_entity_address_line3", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_address_line3)", "feed_expr": None},
    {"canonical": "legal_entity_city_code", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_city_code)", "feed_expr": None},
    {"canonical": "legal_entity_foreign_city", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_foreign_city)", "feed_expr": None},
    {"canonical": "legal_entity_foreign_descr", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_foreign_descr)", "feed_expr": None},
    {"canonical": "legal_entity_foreign_posta", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_foreign_posta)", "feed_expr": None},
    {"canonical": "legal_entity_foreign_provi", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_foreign_provi)", "feed_expr": None},
    {"canonical": "legal_entity_zip4", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_zip4)", "feed_expr": None},
    {"canonical": "legal_entity_zip_last4", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(legal_entity_zip_last4)", "feed_expr": None},
    {"canonical": "limited_liability_corporat", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "limited_liability_corporat", "feed_expr": None},
    {"canonical": "local_area_set_aside", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(local_area_set_aside)", "feed_expr": None},
    {"canonical": "local_area_set_aside_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(local_area_set_aside_desc)", "feed_expr": None},
    {"canonical": "local_government_owned", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "local_government_owned", "feed_expr": None},
    {"canonical": "manufacturer_of_goods", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "manufacturer_of_goods", "feed_expr": None},
    {"canonical": "materials_supplies_article", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(materials_supplies_article)", "feed_expr": None},
    {"canonical": "materials_supplies_descrip", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(materials_supplies_descrip)", "feed_expr": None},
    {"canonical": "minority_institution", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "minority_institution", "feed_expr": None},
    {"canonical": "minority_owned_business", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "minority_owned_business", "feed_expr": None},
    {"canonical": "multi_year_contract_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(multi_year_contract_desc)", "feed_expr": None},
    {"canonical": "multiple_or_single_aw_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(multiple_or_single_aw_desc)", "feed_expr": None},
    {"canonical": "municipality_local_governm", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "municipality_local_governm", "feed_expr": None},
    {"canonical": "national_interest_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(national_interest_desc)", "feed_expr": None},
    {"canonical": "native_american_owned_busi", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "native_american_owned_busi", "feed_expr": None},
    {"canonical": "native_hawaiian_owned_busi", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "native_hawaiian_owned_busi", "feed_expr": None},
    {"canonical": "native_hawaiian_servicing", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "native_hawaiian_servicing", "feed_expr": None},
    {"canonical": "non_federal_funding_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "non_federal_funding_amount", "feed_expr": None},
    {"canonical": "nonprofit_organization", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "nonprofit_organization", "feed_expr": None},
    {"canonical": "officer_1_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "officer_1_amount", "feed_expr": None},
    {"canonical": "officer_1_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(officer_1_name)", "feed_expr": None},
    {"canonical": "officer_2_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "officer_2_amount", "feed_expr": None},
    {"canonical": "officer_2_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(officer_2_name)", "feed_expr": None},
    {"canonical": "officer_3_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "officer_3_amount", "feed_expr": None},
    {"canonical": "officer_3_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(officer_3_name)", "feed_expr": None},
    {"canonical": "officer_4_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "officer_4_amount", "feed_expr": None},
    {"canonical": "officer_4_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(officer_4_name)", "feed_expr": None},
    {"canonical": "officer_5_amount", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "officer_5_amount", "feed_expr": None},
    {"canonical": "officer_5_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(officer_5_name)", "feed_expr": None},
    {"canonical": "organizational_type", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(organizational_type)", "feed_expr": None},
    {"canonical": "original_loan_subsidy_cost", "duck_type": "DOUBLE", "group": "enrich", "bulk_expr": "original_loan_subsidy_cost", "feed_expr": None},
    {"canonical": "other_minority_owned_busin", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "other_minority_owned_busin", "feed_expr": None},
    {"canonical": "other_not_for_profit_organ", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "other_not_for_profit_organ", "feed_expr": None},
    {"canonical": "other_statutory_authority", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(other_statutory_authority)", "feed_expr": None},
    {"canonical": "other_than_full_and_o_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(other_than_full_and_o_desc)", "feed_expr": None},
    {"canonical": "parent_recipient_name_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(parent_recipient_name_raw)", "feed_expr": None},
    {"canonical": "parent_recipient_unique_id", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(parent_recipient_unique_id)", "feed_expr": None},
    {"canonical": "partnership_or_limited_lia", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "partnership_or_limited_lia", "feed_expr": None},
    {"canonical": "performance_based_se_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(performance_based_se_desc)", "feed_expr": None},
    {"canonical": "period_of_perf_potential_e", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(period_of_perf_potential_e)", "feed_expr": None},
    {"canonical": "place_of_manufacture", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(place_of_manufacture)", "feed_expr": None},
    {"canonical": "place_of_manufacture_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(place_of_manufacture_desc)", "feed_expr": None},
    {"canonical": "place_of_perform_zip_last4", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(place_of_perform_zip_last4)", "feed_expr": None},
    {"canonical": "place_of_performance_zip4a", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(place_of_performance_zip4a)", "feed_expr": None},
    {"canonical": "planning_commission", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "planning_commission", "feed_expr": None},
    {"canonical": "pop_congressional_code_current", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pop_congressional_code_current)", "feed_expr": None},
    {"canonical": "pop_congressional_population", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "pop_congressional_population", "feed_expr": None},
    {"canonical": "pop_country_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pop_country_name)", "feed_expr": None},
    {"canonical": "pop_county_code", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pop_county_code)", "feed_expr": None},
    {"canonical": "pop_county_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pop_county_name)", "feed_expr": None},
    {"canonical": "pop_county_population", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "pop_county_population", "feed_expr": None},
    {"canonical": "pop_state_fips", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pop_state_fips)", "feed_expr": None},
    {"canonical": "pop_state_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pop_state_name)", "feed_expr": None},
    {"canonical": "pop_state_population", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "pop_state_population", "feed_expr": None},
    {"canonical": "port_authority", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "port_authority", "feed_expr": None},
    {"canonical": "potential_total_value_awar", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(potential_total_value_awar)", "feed_expr": None},
    {"canonical": "private_university_or_coll", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "private_university_or_coll", "feed_expr": None},
    {"canonical": "program_system_or_equ_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(program_system_or_equ_desc)", "feed_expr": None},
    {"canonical": "program_system_or_equipmen", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(program_system_or_equipmen)", "feed_expr": None},
    {"canonical": "published_fabs_id", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "published_fabs_id", "feed_expr": None},
    {"canonical": "pulled_from", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(pulled_from)", "feed_expr": None},
    {"canonical": "purchase_card_as_paym_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(purchase_card_as_paym_desc)", "feed_expr": None},
    {"canonical": "receives_contracts_and_gra", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "receives_contracts_and_gra", "feed_expr": None},
    {"canonical": "recipient_location_congressional_code_current", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_location_congressional_code_current)", "feed_expr": None},
    {"canonical": "recipient_location_congressional_population", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "recipient_location_congressional_population", "feed_expr": None},
    {"canonical": "recipient_location_country_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_location_country_name)", "feed_expr": None},
    {"canonical": "recipient_location_county_code", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_location_county_code)", "feed_expr": None},
    {"canonical": "recipient_location_county_population", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "recipient_location_county_population", "feed_expr": None},
    {"canonical": "recipient_location_state_fips", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_location_state_fips)", "feed_expr": None},
    {"canonical": "recipient_location_state_name", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_location_state_name)", "feed_expr": None},
    {"canonical": "recipient_location_state_population", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "recipient_location_state_population", "feed_expr": None},
    {"canonical": "recipient_name_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_name_raw)", "feed_expr": None},
    {"canonical": "recipient_unique_id", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recipient_unique_id)", "feed_expr": None},
    {"canonical": "record_type", "duck_type": "BIGINT", "group": "enrich", "bulk_expr": "record_type", "feed_expr": None},
    {"canonical": "record_type_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(record_type_description)", "feed_expr": None},
    {"canonical": "recovered_materials_s_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recovered_materials_s_desc)", "feed_expr": None},
    {"canonical": "recovered_materials_sustai", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(recovered_materials_sustai)", "feed_expr": None},
    {"canonical": "referenced_idv_agency_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(referenced_idv_agency_desc)", "feed_expr": None},
    {"canonical": "referenced_idv_type_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(referenced_idv_type_desc)", "feed_expr": None},
    {"canonical": "referenced_mult_or_si_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(referenced_mult_or_si_desc)", "feed_expr": None},
    {"canonical": "referenced_mult_or_single", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(referenced_mult_or_single)", "feed_expr": None},
    {"canonical": "research", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(research)", "feed_expr": None},
    {"canonical": "research_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(research_description)", "feed_expr": None},
    {"canonical": "sai_number", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(sai_number)", "feed_expr": None},
    {"canonical": "sam_exception", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(sam_exception)", "feed_expr": None},
    {"canonical": "sam_exception_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(sam_exception_description)", "feed_expr": None},
    {"canonical": "sba_certified_8_a_joint_ve", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "sba_certified_8_a_joint_ve", "feed_expr": None},
    {"canonical": "school_district_local_gove", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "school_district_local_gove", "feed_expr": None},
    {"canonical": "school_of_forestry", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "school_of_forestry", "feed_expr": None},
    {"canonical": "sea_transportation", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(sea_transportation)", "feed_expr": None},
    {"canonical": "sea_transportation_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(sea_transportation_desc)", "feed_expr": None},
    {"canonical": "self_certified_small_disad", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "self_certified_small_disad", "feed_expr": None},
    {"canonical": "small_agricultural_coopera", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "small_agricultural_coopera", "feed_expr": None},
    {"canonical": "small_business_competitive", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "small_business_competitive", "feed_expr": None},
    {"canonical": "small_disadvantaged_busine", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "small_disadvantaged_busine", "feed_expr": None},
    {"canonical": "sole_proprietorship", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "sole_proprietorship", "feed_expr": None},
    {"canonical": "solicitation_procedur_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(solicitation_procedur_desc)", "feed_expr": None},
    {"canonical": "source_schema", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(source_schema)", "feed_expr": None},
    {"canonical": "source_table", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(source_table)", "feed_expr": None},
    {"canonical": "state_controlled_instituti", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "state_controlled_instituti", "feed_expr": None},
    {"canonical": "subchapter_s_corporation", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "subchapter_s_corporation", "feed_expr": None},
    {"canonical": "subcontinent_asian_asian_i", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "subcontinent_asian_asian_i", "feed_expr": None},
    {"canonical": "subcontracting_plan_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(subcontracting_plan_desc)", "feed_expr": None},
    {"canonical": "tas_components", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(tas_components)", "feed_expr": None},
    {"canonical": "the_ability_one_program", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "the_ability_one_program", "feed_expr": None},
    {"canonical": "township_local_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "township_local_government", "feed_expr": None},
    {"canonical": "transit_authority", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "transit_authority", "feed_expr": None},
    {"canonical": "tribal_college", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "tribal_college", "feed_expr": None},
    {"canonical": "tribally_owned_business", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "tribally_owned_business", "feed_expr": None},
    {"canonical": "type", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type)", "feed_expr": None},
    {"canonical": "type_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_description)", "feed_expr": None},
    {"canonical": "type_description_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_description_raw)", "feed_expr": None},
    {"canonical": "type_of_contract_pric_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_of_contract_pric_desc)", "feed_expr": None},
    {"canonical": "type_of_idc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_of_idc)", "feed_expr": None},
    {"canonical": "type_of_idc_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_of_idc_description)", "feed_expr": None},
    {"canonical": "type_raw", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_raw)", "feed_expr": None},
    {"canonical": "type_set_aside_description", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(type_set_aside_description)", "feed_expr": None},
    {"canonical": "undefinitized_action_desc", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(undefinitized_action_desc)", "feed_expr": None},
    {"canonical": "update_date", "duck_type": "TIMESTAMP", "group": "enrich", "bulk_expr": "update_date", "feed_expr": None},
    {"canonical": "uri", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(uri)", "feed_expr": None},
    {"canonical": "us_federal_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "us_federal_government", "feed_expr": None},
    {"canonical": "us_government_entity", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "us_government_entity", "feed_expr": None},
    {"canonical": "us_local_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "us_local_government", "feed_expr": None},
    {"canonical": "us_state_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "us_state_government", "feed_expr": None},
    {"canonical": "us_tribal_government", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "us_tribal_government", "feed_expr": None},
    {"canonical": "usaspending_snapshot_date", "duck_type": "DATE", "group": "enrich", "bulk_expr": "usaspending_snapshot_date", "feed_expr": None},
    {"canonical": "usaspending_unique_transaction_id", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(usaspending_unique_transaction_id)", "feed_expr": None},
    {"canonical": "vendor_doing_as_business_n", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(vendor_doing_as_business_n)", "feed_expr": None},
    {"canonical": "vendor_fax_number", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(vendor_fax_number)", "feed_expr": None},
    {"canonical": "vendor_phone_number", "duck_type": "VARCHAR", "group": "enrich", "bulk_expr": "s(vendor_phone_number)", "feed_expr": None},
    {"canonical": "veteran_owned_business", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "veteran_owned_business", "feed_expr": None},
    {"canonical": "veterinary_college", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "veterinary_college", "feed_expr": None},
    {"canonical": "veterinary_hospital", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "veterinary_hospital", "feed_expr": None},
    {"canonical": "woman_owned_business", "duck_type": "BOOLEAN", "group": "enrich", "bulk_expr": "woman_owned_business", "feed_expr": None},
    # ── end OBT expansion ────────────────────────────────────────────────────────────
    # ---- (d) provenance ----
    {"canonical": "canonical_source", "duck_type": "VARCHAR", "group": "prov",
     "bulk_expr": None, "feed_expr": None},   # literal per leg
    {"canonical": "built_at", "duck_type": "TIMESTAMP", "group": "prov",
     "bulk_expr": None, "feed_expr": None},   # Python-injected literal per leg
]

_MACROS = """
CREATE MACRO s(x) AS nullif(nullif(trim(x), ''), '-NONE-');
CREATE MACRO kbulk(detached, txnuid) AS s(COALESCE(s(detached), s(txnuid)));
"""

# Index plan (design §4) — program-derived presence-filtered at index() time.
BTREE_COLS = ["contract_transaction_unique_key", "contract_award_unique_key", "recipient_uei",
              "action_date", "last_modified_date", "naics_code", "product_or_service_code",
              "federal_action_obligation", "recipient_hash", "award_id_piid", "pop_county_fips"]
BITMAP_COLS = ["action_date_fiscal_year", "type_of_set_aside_code", "awarding_agency_code",
               "award_type_code", "idv_type_code", "canonical_source", "subcontracting_plan"]


# ---- COLUMN_SPEC derived helpers (all generated; nothing hand-transcribed) ---- #
def _canon_order() -> list[str]:
    return [c["canonical"] for c in COLUMN_SPEC]


def _cols(group: str) -> list[dict]:
    return [c for c in COLUMN_SPEC if c["group"] == group]


def _typed_null(c: dict) -> str:
    return f"CAST(NULL AS {c['duck_type']})"


def _bulk_source_cols() -> list[str]:
    """Distinct raw BULK column names referenced by the BULK projection — the scanner column list.
    Parsed from bulk_expr by stripping s()/kbulk()/TRY_CAST wrappers down to bare identifiers."""
    import re
    raw: set[str] = set()
    for c in COLUMN_SPEC:
        expr = c["bulk_expr"]
        if not expr:
            continue
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr):
            if tok in ("s", "kbulk", "TRY_CAST", "CAST", "COALESCE", "AS", "DOUBLE", "BIGINT",
                       "DATE", "TIMESTAMP", "VARCHAR", "INTEGER", "replace", "upper", "substr", "lower"):
                continue
            raw.add(tok)
    return sorted(raw)


# ----- canonical-vocabulary scanner column list for the FRESH feed ----- #
def _feed_source_cols() -> list[str]:
    """Raw canonical-vocabulary columns the FRESH projection reads (keys + core only; enrichment is
    BULK-only). Parsed from feed_expr. Presence-filtered against the live FRESH schema in build()."""
    import re
    raw: set[str] = set()
    for c in COLUMN_SPEC:
        expr = c["feed_expr"]
        if not expr:
            continue
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr):
            if tok in ("s", "TRY_CAST", "CAST", "AS", "DOUBLE", "BIGINT", "DATE", "TIMESTAMP",
                       "VARCHAR", "INTEGER", "replace", "upper", "substr", "lower"):
                continue
            raw.add(tok)
    return sorted(raw)


# =========================================================================================== #
# Generated SQL builders (pure strings — NO R2; safe to print for inspection)
# =========================================================================================== #
def _proj_select(side: str, built_at_iso: str) -> str:
    """Generate ONE per-source projection SELECT body in the canonical column order.
    side: 'bulk' | 'feed'. For 'feed', enrichment columns project as typed NULL placeholders.
    Provenance: built_at injected literal ONLY. canonical_source is NOT projected here — it is
    derived per-key from the winning core row's src tag in `resolved` (Variant B, INV-7), so the
    three projection legs omit canonical_source IDENTICALLY and the schema-identity gate (run on the
    collapsed tables, which carry the projections' exact shapes) still passes."""
    lines = []
    for c in COLUMN_SPEC:
        canon = c["canonical"]
        if c["group"] == "prov":
            if canon == "canonical_source":
                # Derived downstream (w.src AS canonical_source in `resolved`); projected as a typed
                # NULL placeholder here so the projection carries the column in the locked order and
                # the schema-identity gate compares it identically across all three legs.
                lines.append(f"  {_typed_null(c)} AS canonical_source")
            else:  # built_at
                lines.append(f"  TIMESTAMP '{built_at_iso}' AS built_at")
            continue
        if side == "bulk":
            expr = c["bulk_expr"] if c["bulk_expr"] is not None else _typed_null(c)
        else:  # feed
            expr = c["feed_expr"] if c["feed_expr"] is not None else _typed_null(c)
        lines.append(f"  {expr} AS {canon}")
    return ",\n".join(lines)


def _enrich_replace_block() -> str:
    """The §3.7 enrichment REPLACE block for `resolved`: overwrite every enrichment column keyed on
    the transaction key, INDEPENDENT of which source won the volatile core. PROGRAM-GENERATED from
    COLUMN_SPEC — no hand-transcription. The volatile-core winner is already resolved in
    `core_winner`, so this block touches ONLY the enrichment half.

    2-source (BULK+FRESH) build: MONTHLY re-integration is owned by a parallel agent. EVERY enrichment
    column projects plain b.<col> from bulk_latest (pg) — there is no monthly leg and no COALESCE. The
    12 monthly-unique cols have bulk_expr=None → b.<col> = CAST(NULL AS <type>) (typed-NULL placeholder,
    100% NULL in the live dataset), reserved for the monthly agent's re-add. Do NOT resurrect the
    monthly COALESCE / monthly_enrich_latest here — re-author against the renamed monthly upstream when
    it lands (see the module MERGE header)."""
    parts = []
    for c in _cols("enrich"):
        col = c["canonical"]
        parts.append(f"    b.{col} AS {col}")
    return ",\n".join(parts)


def _stage1_sql(built_at_iso: str) -> str:
    """STAGE 1 — macros + the two per-source collapses (BULK + FRESH), with the bulk/fresh projections
    INLINED into their collapse windows (P1-4 spill hygiene: the ~107M-row bulk_proj duplicate
    materialization was the single largest spill contributor — 246 GB observed on-disk before the
    collapses even completed on the first full-build attempt). Executed as ONE multi-statement script;
    the schema-identity gate then runs against the two COLLAPSED tables (identical canonical
    NAME+ORDER+TYPE by construction — collapse = SELECT * EXCLUDE (rn) over a projection-shaped inner)
    before the stage-2 merge.

    2-source (BULK+FRESH) build: no MONTHLY/archive projection or collapse. Also captured here, as
    1-row m_* tables, every metric whose source table is dropped in stage 2 (free-as-you-go DROPs)."""
    bulk_proj = _proj_select("bulk", built_at_iso)
    fresh_proj = _proj_select("feed", built_at_iso)
    return f"""{_MACROS}
-- ===== §3.2 FRESH dedup → latest-per-key (projection INLINED; fresh_proj never materialized) ===== --
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY contract_transaction_unique_key
            ORDER BY last_modified_date DESC NULLS LAST,
                     (federal_action_obligation IS NULL) ASC,
                     modification_number DESC NULLS LAST,
                     contract_award_unique_key DESC NULLS LAST) AS rn
  FROM (
    SELECT
{fresh_proj}
    FROM fresh_r
  )
  WHERE contract_transaction_unique_key IS NOT NULL
) WHERE rn = 1;

-- ===== §3.3 single bulk_latest collapse (projection INLINED; ONE 107M scan, bulk_proj never
-- materialized). Enrichment-maximizing deterministic dedup: latest mtime, then prefer populated
-- enrichment, then stable transaction_id surrogate. bulk_latest is the SOLE pg enrichment source
-- (role B) AND a competitor in the core resolution (role A/C). ===== --
CREATE TEMP TABLE bulk_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *,
         row_number() OVER (
           PARTITION BY contract_transaction_unique_key
           ORDER BY last_modified_date DESC NULLS LAST,
                    (recipient_hash IS NULL) ASC,
                    transaction_id DESC NULLS LAST
         ) AS rn
  FROM (
    SELECT
{bulk_proj}
    FROM bulk_r
  )
  WHERE contract_transaction_unique_key IS NOT NULL
) WHERE rn = 1;

-- ===== early metric captures (1-row each) — sources dropped at stage-2 boundaries ===== --
CREATE TEMP TABLE m_rows_in_fresh AS SELECT count(*) AS c FROM fresh_r;
-- BULK is consumed exactly once by the inlined collapse (single-pass reader) — the raw scanned
-- count cannot be re-taken. BULK is proven PK-unique (107,250,527 distinct == rowcount), so the
-- post-collapse count equals rows scanned; if BULK ever grows dup keys, dedup_collapsed
-- undercounts by exactly those dups (documented, not silent).
CREATE TEMP TABLE m_rows_in_bulk AS SELECT count(*) AS c FROM bulk_latest;
CREATE TEMP TABLE m_fresh_only_tail AS
SELECT count(*) AS c FROM fresh_latest f
ANTI JOIN bulk_latest b ON f.contract_transaction_unique_key = b.contract_transaction_unique_key;
"""


DELTA_STAMP_COL = "archive_snapshot_stamp"


def _stage2_sql() -> str:
    """STAGE 2 — the merge: 2-source (BULK + FRESH) reconciliation, ONE physical artifact. Pipeline:
      (stage-1 collapses: fresh_latest / bulk_latest, ≤1 row per key each)
        → core_union   (UNION ALL BY NAME of the two collapsed CORES, each tagged src+source_rank)
        → core_winner  (SINGLE 2-way window: argmax(last_modified_date) per key, total-order tiebreak;
                        source_rank FRESH(1) < BULK(2) → equal-mtime tie → FRESH wins)
        → resolved     (LEFT JOIN bulk_latest [pg] → pg-only enrichment REPLACE + w.src AS canonical_source)
        → canonical_out(locked canonical projection; NO tombstone/reinstatement in a 2-source build)

    P1-4 SPILL HYGIENE: every giant TEMP TABLE is dropped at its last-reader boundary (free-as-you-go),
    and the metrics whose sources are dropped are captured first as 1-row m_* tables (stage 1 +
    m_merged here). Peak concurrent spill is bounded by the join inputs of the widest single statement
    (~2 wide ~107M-row relations at `resolved`), not by the sum of every intermediate.

    CORE resolution: argmax(last_modified_date) over {FRESH, BULK}. After the two upstream collapses
    there is AT MOST one row per source per key, so source_rank alone disambiguates every cross-source
    mtime tie (FRESH=1 < BULK=2 → tie → FRESH). PK-uniqueness is structural (row_number()=1 over
    ≤1-per-source collapses). Pure string; references the stage-1 collapses. Executed as ONE
    multi-statement script.

    2-source scope: MONTHLY re-integration is owned by a parallel agent; the 12 monthly-unique enrich
    cols are typed-NULL placeholders (feed_expr=None AND bulk_expr=None) reserved for that re-add. Do
    NOT resurrect archive_proj/monthly_latest/monthly_enrich_latest/tombstone here — re-author against
    the renamed monthly upstream when it lands (see the module MERGE header)."""
    enrich_block = _enrich_replace_block()
    canon_cols = ", ".join(_canon_order())
    return f"""-- ===== §3.5 two collapsed CORES → vertical union, each tagged src + source_rank ===== --
-- 2-source (BULK+FRESH) build. The monthly leg is owned by a parallel agent; the 12
-- monthly-unique enrich cols (COLUMN_SPEC ~lines 309-332) are typed-NULL placeholders reserved for
-- that re-add. Keep this build strictly BULK + FRESH; the monthly leg is re-authored elsewhere.
-- src CAST identically as VARCHAR and source_rank as INTEGER in both arms so BY-NAME union types
-- align. source_rank encodes the locked precedence FRESH(1) > BULK(2): equal-mtime tie → FRESH.
CREATE TEMP TABLE core_union AS
SELECT CAST('fresh' AS VARCHAR) AS src, CAST(1 AS INTEGER) AS source_rank, f.* FROM fresh_latest f
UNION ALL BY NAME
SELECT CAST('bulk'  AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, b.* FROM bulk_latest b;

-- P1-4 boundary: core_union was the last reader of fresh_latest (its metric was captured in stage 1).
-- bulk_latest LIVES ON (enrichment source at §3.7).
DROP TABLE fresh_latest;

-- ===== §3.6 SINGLE 2-way per-key core resolution: argmax(last_modified_date) ===== --
-- Provably total order: after the two upstream collapses there is AT MOST one row per source per key,
-- so source_rank alone disambiguates every cross-source mtime tie; the trailing award-key term is
-- defense-in-depth. NULL mtime sorts LAST (= oldest) per BLOCKER-1. Exactly one survivor per key
-- (row_number()=1) → PK-uniqueness is structural.
CREATE TEMP TABLE core_winner AS
SELECT * EXCLUDE (rn, source_rank) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY contract_transaction_unique_key
            ORDER BY last_modified_date DESC NULLS LAST,
                     source_rank ASC,
                     contract_award_unique_key DESC NULLS LAST) AS rn
  FROM core_union
) WHERE rn = 1;

-- P1-4 boundary: core_winner supersedes core_union. Capture the merged-count metric first.
CREATE TEMP TABLE m_merged AS SELECT count(*) AS c FROM core_winner;
DROP TABLE core_union;

-- ===== §3.7 enrichment fill: pg (bulk_latest) ONLY ===== --
-- The LEFT JOIN is to a PK-unique per-key collapse → no fan-out. The enrichment REPLACE (§3.7 builder)
-- overwrites the enrich half with plain b.<col> (pg-only); the 12 monthly-unique cols resolve to
-- CAST(NULL AS <type>) (bulk_expr None). b is NULL for fresh-only keys. canonical_source is derived
-- HERE, exactly once, as the winning core row's src tag (INV-7) — the true per-key winner (fresh|bulk),
-- never a partition literal. EXCLUDE the placeholder canonical_source carried up from the projections
-- (typed NULL) as well as src, then re-derive canonical_source := w.src. Excluding the placeholder is
-- REQUIRED: keeping it would collide with `w.src AS canonical_source` and DuckDB would silently rename
-- the derived column (canonical_source_1), leaving the locked-order projection to read the all-NULL
-- placeholder.
CREATE TEMP TABLE resolved AS
SELECT
  w.* EXCLUDE (src, canonical_source) REPLACE (
{enrich_block}
  ),
  w.src AS canonical_source
FROM core_winner w
LEFT JOIN bulk_latest b ON w.contract_transaction_unique_key = b.contract_transaction_unique_key;

-- P1-4 boundary: resolved supersedes core_winner + the enrichment leg. rows_in_bulk was captured in
-- stage 1 (m_rows_in_bulk).
DROP TABLE core_winner;
DROP TABLE bulk_latest;

-- ===== §3.8 locked canonical projection → canonical_out (NO tombstone in a 2-source build) ===== --
CREATE TEMP TABLE canonical_out AS
SELECT {canon_cols} FROM resolved;

DROP TABLE resolved;
"""


def _build_merge_sql(*, built_at_iso: str, since: str | None) -> str:
    """The FULL merge SQL (stage 1 + stage 2) concatenated — for inspection / print_merge_sql ONLY.
    build() executes _stage1_sql() and _stage2_sql() separately so the schema-identity gate can run
    between them (against the collapsed tables). No R2 access here; safe to print.

    --since note: the predicate is pushed into the TWO DATA scanners (BULK + FRESH, caller/build side).
    Carried here only as a comment marker for traceability."""
    since_note = (f"-- --since={since} pushed into the TWO data scanners (BULK + FRESH)\n"
                  if since else "")
    return (since_note + _stage1_sql(built_at_iso)
            + "\n-- [build() runs the schema-identity gate HERE, on the collapsed tables]\n"
            + _stage2_sql())


def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _r2_so() -> dict[str, str]:
    ep = os.environ.get("R2_ENDPOINT")
    acct = os.environ.get("R2_ACCOUNT_ID")
    if not ep and acct:
        ep = f"https://{acct}.r2.cloudflarestorage.com"
    if not ep:
        raise RuntimeError("Set R2_ENDPOINT or R2_ACCOUNT_ID.")
    return {"aws_access_key_id": os.environ["R2_ACCESS_KEY_ID"],
            "aws_secret_access_key": os.environ["R2_SECRET_ACCESS_KEY"],
            "endpoint": ep, "region": "auto"}


def _s3():
    import boto3
    from botocore.config import Config
    so = _r2_so()
    return boto3.client("s3", endpoint_url=so["endpoint"],
                        aws_access_key_id=so["aws_access_key_id"],
                        aws_secret_access_key=so["aws_secret_access_key"], region_name="auto",
                        config=Config(retries={"max_attempts": 10, "mode": "standard"},
                                      connect_timeout=30, read_timeout=120,
                                      request_checksum_calculation="when_required",
                                      response_checksum_validation="when_required"))


def _publish_local_to_r2(s3, uri, local_ds) -> int:
    """Replace the R2 dataset prefix with a local Lance dataset, uploaded file-by-file (boto3
    s3transfer uniform multipart parts, R2-compliant; retries each part). Prior-prefix wipe uses
    DeleteObjects in batches of <=1000 keys (the API hard cap)."""
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    pag = s3.get_paginator("list_objects_v2")
    to_del: list[dict] = []
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            to_del.append({"Key": o["Key"]})
            if len(to_del) >= 1000:
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                to_del = []
    if to_del:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
    uploaded = 0
    for root, _dirs, files in os.walk(local_ds):
        for f in files:
            lp = os.path.join(root, f)
            rel = os.path.relpath(lp, local_ds).replace(os.sep, "/")
            s3.upload_file(lp, BUCKET, prefix + rel)
            uploaded += 1
    return uploaded


def _relset(root) -> set[str]:
    """Every file under `root` as POSIX-relative paths — snapshot primitive for delta publishing."""
    out: set[str] = set()
    for r, _dirs, files in os.walk(root):
        for f in files:
            out.add(os.path.relpath(os.path.join(r, f), root).replace(os.sep, "/"))
    return out


def _download_r2_to_local(s3, uri, local_dir, workers: int = 16) -> int:
    """Mirror an R2 dataset prefix → local dir (inverse of _publish_local_to_r2), preserving relative
    layout so the copy opens as a byte-identical Lance dataset. Parallel per-file: the ~90 GiB mirror
    is dominated by per-object latency, so serial download_file stalls the whole index stage ~40 min.
    A shared boto3 client is thread-safe for download_file; per-file multipart concurrency is capped so
    workers × chunks stays bounded. R2's uniform-part rule is a WRITE constraint only — reads are free."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from boto3.s3.transfer import TransferConfig
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    cfg = TransferConfig(max_concurrency=4, multipart_threshold=64 * 1024**2,
                         multipart_chunksize=64 * 1024**2)
    keys: list[str] = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            if o["Key"][len(prefix):]:  # skip the prefix placeholder key, if any
                keys.append(o["Key"])
    for k in keys:  # pre-create dirs single-threaded to avoid makedirs races in workers
        os.makedirs(os.path.dirname(os.path.join(local_dir, k[len(prefix):].replace("/", os.sep)))
                    or local_dir, exist_ok=True)

    def _get(key: str) -> None:
        s3.download_file(BUCKET, key, os.path.join(local_dir, key[len(prefix):].replace("/", os.sep)),
                         Config=cfg)

    done = 0
    log(f"  mirroring {len(keys)} objects ({workers}-way)…")
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_get, k) for k in keys]
        for f in as_completed(futs):
            f.result()  # re-raise any worker failure (fail-closed: a partial mirror must abort)
            done += 1
            if done % 20 == 0 or done == len(keys):
                log(f"  mirrored {done}/{len(keys)} files")
    return done


def _publish_index_delta(s3, uri, local_ds, before: set[str]) -> int:
    """Append-only publish of an index build: upload ONLY files created since `before` (new
    _indices/<uuid>/**) PLUS every manifest/version/transaction file (small, always refreshed so the
    R2 latest-version pointer advances regardless of Lance's manifest-naming scheme). Data fragments —
    large, unchanged, present in `before`, non-meta — are NEVER re-uploaded or deleted. boto3
    upload_file ⇒ uniform multipart parts (R2-compliant); the native Lance R2 writer is bypassed."""
    prefix = uri.replace(f"s3://{BUCKET}/", "")

    def _is_meta(rel: str) -> bool:
        return (rel.startswith("_versions/") or rel.startswith("_transactions/")
                or rel.endswith(".manifest") or "_latest" in rel)

    cur = _relset(local_ds)
    to_pub = sorted(r for r in cur if r not in before or _is_meta(r))
    for rel in to_pub:
        s3.upload_file(os.path.join(local_ds, rel.replace("/", os.sep)), BUCKET, prefix + rel)
    return len(to_pub)


def _gc_orphan_indices(s3, uri, keep_uuids: set[str]) -> int:
    """Remove _indices/<uuid>/ prefixes not referenced by the live manifest (e.g. the half-written dir
    a failed native-R2 index attempt leaves behind) and abort dangling multipart uploads under the
    dataset prefix (a failed native write leaks an open MPU → silent storage charge). Best-effort:
    self-healing hygiene must never raise into — nor fail — the index publish it follows."""
    prefix = uri.replace(f"s3://{BUCKET}/", "")
    idx_prefix = prefix + "_indices/"
    removed = 0
    try:
        pag = s3.get_paginator("list_objects_v2")
        to_del: list[dict] = []
        for page in pag.paginate(Bucket=BUCKET, Prefix=idx_prefix):
            for o in page.get("Contents", []):
                uuid = o["Key"][len(idx_prefix):].split("/", 1)[0]
                if uuid and uuid not in keep_uuids:
                    to_del.append({"Key": o["Key"]})
                    if len(to_del) >= 1000:
                        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
                        removed += len(to_del)
                        to_del = []
        if to_del:
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": to_del, "Quiet": True})
            removed += len(to_del)
    except Exception as exc:  # noqa: BLE001 — hygiene never breaks the publish
        log(f"WARN: orphan-index GC skipped: {exc}")
    try:
        mpus = s3.list_multipart_uploads(Bucket=BUCKET, Prefix=prefix).get("Uploads", [])
        for u in mpus:
            s3.abort_multipart_upload(Bucket=BUCKET, Key=u["Key"], UploadId=u["UploadId"])
        if mpus:
            log(f"aborted {len(mpus)} dangling multipart upload(s)")
    except Exception as exc:  # noqa: BLE001
        log(f"WARN: MPU abort skipped: {exc}")
    return removed


def _build_indices_local(local_ds: str) -> list[str]:
    """Build the §4 BTREE/BITMAP scalar indices against a LOCAL Lance path; return the columns actually
    indexed (schema-presence filtered). Opened with lance.dataset(local_ds) and NO storage_options → the
    local-FS writer (no multipart), the ONLY R2-safe way to write indices (the native R2 object-writer
    streams adaptive-sized parts R2 rejects: 400 InvalidPart, 'all non-trailing parts must have the same
    length'). Shared by build() (indices written into local_ds BEFORE the single _publish_local_to_r2 →
    published atomically WITH the data) and index() (indices written into the R2→local mirror BEFORE the
    append-only delta publish). Idempotent: replace=True rebuilds cleanly; the TypeError fallback covers
    older lance without the kwarg. Raises on the first failing column so callers fail-closed BEFORE any
    R2 mutation."""
    import lance
    ds = lance.dataset(local_ds)                     # LOCAL — no storage_options, no R2 writer
    present = set(ds.schema.names)
    built: list[str] = []
    log(f"indexing LOCAL ({ds.count_rows():,} rows)")
    for col in [c for c in BTREE_COLS if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        log(f"  BTREE ✓ {col}")
    for col in [c for c in BITMAP_COLS if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BITMAP")
        built.append(col)
        log(f"  BITMAP ✓ {col}")
    return built


def _dataset_exists(uri, so) -> bool:
    import lance
    try:
        lance.dataset(uri, storage_options=so)
        return True
    except Exception:  # noqa: BLE001
        return False


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={DUCK_THREADS}")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _record_run(*, rows_in_bulk, rows_in_fresh, rows_in_archive_full, rows_out, dedup_collapsed,
                fresh_only_tail, deletes_tombstoned, monthly_corrections_applied, max_action_date,
                columns, write_mode, indices_built, status, error, started, completed) -> None:
    import psycopg
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    if status != "success" and not error:
        error = "unknown terminal failure (no exception captured)"
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{OPS_TABLE}')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath(OPS_SQL_FILE).read_text())
            cur.execute(
                "INSERT INTO ops.usaspending_fpds_canonical_runs (feed, rows_in_bulk, rows_in_fresh, "
                "rows_in_archive_full, rows_out, dedup_collapsed, fresh_only_tail, deletes_tombstoned, "
                "monthly_corrections_applied, max_action_date, columns, write_mode, indices_built, "
                "status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (FEED, rows_in_bulk, rows_in_fresh, rows_in_archive_full, rows_out, dedup_collapsed,
                 fresh_only_tail, deletes_tombstoned, monthly_corrections_applied, max_action_date,
                 columns, write_mode,
                 ",".join(indices_built) if indices_built else None, status,
                 (error or "")[:2000] or None, started, completed))
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        log(f"WARN: ops.* write failed: {exc}")


def init_ops() -> None:
    import psycopg
    sql = Path(__file__).parent.joinpath(OPS_SQL_FILE).read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    log("ops DDL applied")


def _assert_collapse_schema_identity(con) -> None:
    """§4 enforcement — PROGRAMMATIC, not aspirational. The two per-source COLLAPSES (which are
    SELECT * EXCLUDE (rn) over projection-shaped inners, so they carry the projections' exact
    (name, type) sequences) MUST be byte-identical before any union. Raise on mismatch (hard build
    failure rather than a silent transposition). P1-4 note: the gate moved from the *_proj pair to
    the collapsed tables because the bulk/fresh projections are inlined and never materialized."""
    sigs = {}
    for name in ("bulk_latest", "fresh_latest"):
        rows = con.execute(f"DESCRIBE {name}").fetchall()  # (column_name, column_type, ...)
        sigs[name] = [(r[0], r[1]) for r in rows]
    base = sigs["bulk_latest"]
    for name, sig in sigs.items():
        if sig != base:
            diff = [(b, s) for b, s in zip(base, sig) if b != s]
            raise RuntimeError(
                f"collapse schema mismatch: {name} != bulk_latest. "
                f"first divergences (bulk_latest vs {name}): {diff[:5]}")


def build(since: str | None = None, target_uri: str = CANONICAL_URI) -> dict:
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    # ONE naive-UTC literal, injected into all three projections (NOT now()).
    built_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    built_at_iso = built_at.strftime("%Y-%m-%d %H:%M:%S.%f")

    status, error = "error", None
    rows_in_bulk = rows_in_fresh = rows_in_archive_full = 0
    rows_out = dedup_collapsed = fresh_only_tail = deletes_tombstoned = 0
    monthly_corrections_applied = 0
    max_action_date = None
    metrics: dict = {}
    built_idx: list[str] = []
    con = None
    local_ds = os.path.join(SCRATCH, "canonical_lance")
    try:
        os.makedirs(SCRATCH, exist_ok=True)
        shutil.rmtree(local_ds, ignore_errors=True)

        # --since pushed into the TWO DATA scanners (BULK + FRESH). BULK action_date is date32 → compare
        # to a DATE literal; FRESH action_date is lexical ISO-10 string (0 nulls) → lexical compare.
        bulk_data_filter = f"action_date >= DATE '{since}'" if since else None
        feed_data_filter = f"action_date >= '{since}'" if since else None

        bulk_ds = lance.dataset(BULK_URI, storage_options=so)
        fresh_ds = lance.dataset(FRESH_URI, storage_options=so)

        bulk_present = set(bulk_ds.schema.names)
        feed_keys_core = _feed_source_cols()
        fresh_present = set(fresh_ds.schema.names)

        bulk_scan_cols = [c for c in _bulk_source_cols() if c in bulk_present]
        fresh_scan_cols = [c for c in feed_keys_core if c in fresh_present]

        con = _duck()
        log(f"registering sources (since={since}) → target {target_uri}")
        # BULK: single pass into the per-key collapse → .to_reader().
        con.register("bulk_r", bulk_ds.scanner(columns=bulk_scan_cols,
                                               filter=bulk_data_filter).to_reader())
        # FRESH: deduped then probed (multi-pass) → .to_table() (re-scannable).
        con.register("fresh_r", fresh_ds.scanner(columns=fresh_scan_cols,
                                                 filter=feed_data_filter).to_table())

        # Stage 1: the two collapses (BULK + FRESH; projections INLINED — P1-4), then ENFORCE schema
        # identity on the collapsed tables before the union (§4 — programmatic gate). Stage 2: the
        # 2-source merge with free-as-you-go DROPs; metrics whose sources are dropped were captured as
        # 1-row m_* tables at the correct boundaries.
        con.execute(_stage1_sql(built_at_iso))
        _assert_collapse_schema_identity(con)
        con.execute(_stage2_sql())

        # rows_in_bulk = post-collapse count (BULK proven PK-unique → equals rows scanned; the
        # single-pass reader cannot be re-counted after the inlined collapse — see _stage1_sql).
        rows_in_bulk = con.execute("SELECT c FROM m_rows_in_bulk").fetchone()[0]
        rows_in_fresh = con.execute("SELECT c FROM m_rows_in_fresh").fetchone()[0]

        rows_out = con.execute("SELECT count(*) FROM canonical_out").fetchone()[0]
        pk_total, pk_distinct = con.execute(
            "SELECT count(*), count(DISTINCT contract_transaction_unique_key) FROM canonical_out"
        ).fetchone()
        # FAIL-CLOSED gate — raise BEFORE publish. This single equality is BOTH INV-1 (PK-unique:
        # exactly one row per key) AND INV-4 (rows_out == distinct-key count: landing a correction
        # REPLACES a key's core, never adds/removes a key). rows_out == pk_total by construction, so
        # pk_total == pk_distinct is precisely rows_out == distinct-key count.
        if pk_total != pk_distinct:
            raise RuntimeError(
                f"PK/rows_out gate FAILED (INV-1+INV-4): count(*)={pk_total:,} != "
                f"count(DISTINCT contract_transaction_unique_key)={pk_distinct:,} "
                f"({pk_total - pk_distinct:,} dup keys). Aborting publish.")

        # All intermediate-table metrics come from the 1-row m_* captures (their giant sources are
        # already dropped by the stage-2 P1-4 boundaries; comments on each live at the capture site).
        fresh_only_tail = con.execute("SELECT c FROM m_fresh_only_tail").fetchone()[0]
        # rows_out INVARIANT: distinct-key count of core_union = |FRESH∪BULK keys|; landing a
        # correction REPLACES a key's core, never adds/removes a key. core_winner had exactly that
        # count (one survivor per key) — captured as m_merged before its drop.
        merged_rows = con.execute("SELECT c FROM m_merged").fetchone()[0]
        dedup_collapsed = int(rows_in_bulk + rows_in_fresh - merged_rows)
        max_action_date = con.execute("SELECT max(action_date) FROM canonical_out").fetchone()[0]

        log(f"core_winner={merged_rows:,} rows_out={rows_out:,} fresh_only_tail={fresh_only_tail:,} "
            f"max_action_date={max_action_date}")

        # ── stream the result to a LOCAL Lance dir; boto3-publish (NO direct-R2 write) ──
        reader = con.sql("SELECT * FROM canonical_out").to_arrow_reader(batch_size=200_000)
        log(f"writing Lance LOCALLY → {local_ds}")
        lance.write_dataset(reader, local_ds, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE)
        con.close()
        con = None

        # ── reclaim DuckDB RSS + spill BEFORE the RAM-heavy index sort (fold-isolation fix) ──
        # The standalone index_fn ran the ≥96 GiB LANCE_BYPASS_SPILLING BTREE sort in a FRESH container;
        # folded, that sort runs in the SAME container that just held a DUCK_MEM-limit DuckDB engine. glibc
        # does not return freed arenas to the OS on its own → malloc_trim forces it, so residual DuckDB RSS
        # cannot collide with the sort and trigger an OOM-SIGKILL — an out-of-band kill the except/finally
        # below CANNOT catch (it would leave no ledger row). Dropping DUCK_TMP frees reconcile spill so the
        # local index write cannot ENOSPC the shared ephemeral disk.
        import ctypes
        import gc as _gc
        del reader
        _gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except OSError:
            pass  # non-glibc (e.g. local macOS dev); Modal's debian_slim image is glibc
        shutil.rmtree(DUCK_TMP, ignore_errors=True)

        # ── build §4 indices on the LOCAL dataset BEFORE publish (atomic fold) ──
        # local-FS writer (no multipart, R2-safe); _publish_local_to_r2's os.walk uploads the resulting
        # _indices/ together with the data fragments in ONE publish. A failed index raises HERE, before
        # _s3() and the wipe-then-upload below ever run ⇒ the R2 SoR is never touched (all-or-nothing).
        built_idx = _build_indices_local(local_ds)
        log(f"indices built LOCALLY: {built_idx}")

        s3 = _s3()
        log(f"publishing local Lance (data + indices) → {target_uri} (boto3 uniform-part)…")
        published = _publish_local_to_r2(s3, target_uri, local_ds)
        log(f"published {published} files → {target_uri}")
        status = "success"
        log(f"DONE → {target_uri} rows_out={rows_out:,}")
        metrics = {"target_uri": target_uri, "since": since,
                   "rows_in_bulk": int(rows_in_bulk), "rows_in_fresh": int(rows_in_fresh),
                   "rows_in_archive_full": int(rows_in_archive_full), "rows_out": int(rows_out),
                   "dedup_collapsed": int(dedup_collapsed), "fresh_only_tail": int(fresh_only_tail),
                   "deletes_tombstoned": int(deletes_tombstoned),
                   "monthly_corrections_applied": int(monthly_corrections_applied),
                   "max_action_date": max_action_date, "pk_unique": True,
                   "columns": len(COLUMN_SPEC), "files_published": int(published),
                   "indices_built": built_idx,
                   "write_mode": "overwrite", "status": status}
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        log(f"FAILED: {error}")
        raise
    finally:
        if con is not None:
            con.close()
        _record_run(rows_in_bulk=int(rows_in_bulk), rows_in_fresh=int(rows_in_fresh),
                    rows_in_archive_full=int(rows_in_archive_full), rows_out=int(rows_out),
                    dedup_collapsed=int(dedup_collapsed), fresh_only_tail=int(fresh_only_tail),
                    deletes_tombstoned=int(deletes_tombstoned),
                    monthly_corrections_applied=int(monthly_corrections_applied),
                    max_action_date=max_action_date,
                    columns=len(COLUMN_SPEC), write_mode="overwrite",
                    indices_built=built_idx, status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))
        shutil.rmtree(SCRATCH, ignore_errors=True)
    return metrics


def index(target_uri: str = CANONICAL_URI) -> dict:
    """Build the §4 BTREE/BITMAP indices R2-safely.

    Lance's native object-writer streams adaptive-sized multipart parts; R2 rejects any non-trailing
    part whose size differs (400 InvalidPart: 'All non-trailing parts must have the same length') — the
    IDENTICAL wall the table write hit (header §), so create_scalar_index against the R2 URI dies on the
    first BTREE. The fix mirrors the publish path: (1) mirror the dataset to local ephemeral disk,
    (2) build every index against the LOCAL Lance path (local-FS writer — no multipart), (3) append-only
    publish just the new index + manifest files via boto3 uniform parts (data fragments are never
    rewritten), (4) GC any orphan index dir + dangling MPU from a prior failed native attempt.

    Kept separate from build() for blast-radius isolation (a failed/half index never touches the data
    fragments). Idempotent: replace=True rebuilds cleanly and the GC prunes superseded index UUIDs.
    Columns absent from schema are skipped."""
    s3 = _s3()
    local_ds = os.path.join(SCRATCH, "index_lance")
    shutil.rmtree(local_ds, ignore_errors=True)
    os.makedirs(local_ds, exist_ok=True)
    try:
        log(f"materializing {target_uri} → {local_ds} (boto3 mirror)…")
        got = _download_r2_to_local(s3, target_uri, local_ds)
        log(f"materialized {got} files")
        before = _relset(local_ds)                       # data fragments + committed manifest
        built = _build_indices_local(local_ds)           # LOCAL-FS index build (shared with build())
        after = _relset(local_ds)
        published = _publish_index_delta(s3, target_uri, local_ds, before)
        log(f"published {published} index/manifest file(s) → {target_uri} (append-only)")
        # keep-set = index UUIDs CREATED this run (local _indices/ dirs absent from `before`). The
        # orphan a failed native attempt leaves is present in `before` → excluded → pruned on R2.
        # Filesystem-derived on purpose: GC must not depend on list_indices() attribute names, and an
        # empty keep-set must never be handed to the GC (it would nuke the live indices just published).
        keep = {rel[len("_indices/"):].split("/", 1)[0]
                for rel in (after - before) if rel.startswith("_indices/")}
        pruned = _gc_orphan_indices(s3, target_uri, keep) if keep else 0
        log(f"indices built: {built} (orphan index objects pruned: {pruned})")
        return {"target_uri": target_uri, "indices_built": built,
                "files_published": published, "orphans_pruned": pruned}
    finally:
        shutil.rmtree(local_ds, ignore_errors=True)


def verify(target_uri: str = CANONICAL_URI) -> dict:
    """§5/§7 assertions on read-back. Independent scanner → DuckDB. Set-membership via ANTI JOIN
    (never NOT IN — NULL-poison). Returns JSON with a `pass` verdict and a `failures` list.

    §7 RE-BASELINED CENTERLINES (two-tier reconciliation; monthly corrections land + enrichment fill).
    Absolute row counts scale with --since, so the GATED assertions are scope-independent
    structural invariants; the absolute numbers below are recorded as the full-build (--since NULL)
    reference and the --since 2025-10-01 proof scope, not hard-coded gate thresholds.

      metric                        full-build reference (2-source BULK+FRESH)
      rows_out                      ≈ |FRESH∪BULK| (no monthly / tombstone / reinstatement term)
      fresh_only_tail (keys∉BULK)   ≈ FRESH-only key count                        (scope-dependent)
      canonical_source ∈ {fresh,bulk}; BULK dethroned on every shared key where a newer
        (or equal-mtime, FRESH-ranked-higher) source exists.

    GATES (raise / verdict=fail): INV-1 PK-unique; INV-4 rows_out == distinct-key count;
    built_at_distinct == 1; INV-7 canonical_source domain ⊆ {fresh,bulk}. (MONTHLY / tombstone /
    reinstatement / monthly_corrections_applied gates REMOVED — this is a 2-source BULK+FRESH build;
    monthly re-integration is owned by a parallel agent.)"""
    import lance
    so = _r2_so()
    ds = lance.dataset(target_uri, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    # Two distinct names: register() creates a VIEW named c_src; CREATE TEMP TABLE c then
    # materializes from it. Using the same name for both collides ("View c already exists" on
    # DuckDB 1.5.4). Materialize because the downstream queries multi-scan c, and a single-pass
    # .to_reader() registered directly would exhaust after the first scan (scaffold_ref §6).
    con.register("c_src", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE c AS SELECT * FROM c_src")

    total, distinct = con.execute(
        "SELECT count(*), count(DISTINCT contract_transaction_unique_key) FROM c").fetchone()
    mx_action = con.execute("SELECT max(action_date) FROM c").fetchone()[0]
    src_dist = dict(con.execute(
        "SELECT canonical_source, count(*) FROM c GROUP BY 1 ORDER BY 2 DESC").fetchall())
    # rejected: null_pk is report-only by design (§5 lists it as an invariant to surface, not gate);
    # every leg drops NULL keys via WHERE ... IS NOT NULL, so this is defense-in-depth diagnostics.
    null_pk = con.execute(
        "SELECT count(*) FROM c WHERE contract_transaction_unique_key IS NULL").fetchone()[0]
    built_at_distinct = con.execute("SELECT count(DISTINCT built_at) FROM c").fetchone()[0]
    enrich_on_fresh = con.execute(
        "SELECT count(*) FROM c WHERE canonical_source='fresh' AND recipient_hash IS NOT NULL"
    ).fetchone()[0]
    # canonical_source domain gate (INV-7): only the two legal winner tags may appear (2-source build).
    bad_source_domain = con.execute(
        "SELECT count(*) FROM c WHERE canonical_source IS NULL "
        "OR canonical_source NOT IN ('fresh','bulk')").fetchone()[0]
    con.close()

    failures: list[str] = []
    if total != distinct:
        failures.append(f"INV-1/INV-4 PK-unique+rows_out: count(*)={total:,} != "
                        f"distinct_key={distinct:,} ({total - distinct:,} dup keys)")
    if built_at_distinct != 1:
        failures.append(f"built_at not a single literal: built_at_distinct={built_at_distinct}")
    if bad_source_domain:
        failures.append(f"INV-7 canonical_source domain: {bad_source_domain:,} rows NULL or "
                        f"∉ {{fresh,bulk}}")

    out = {
        "uri": target_uri,
        "rows_out": int(total),
        "pk_unique": bool(total == distinct),
        "pk_dupes": int(total - distinct),
        "null_pk_rows": int(null_pk),
        "max_action_date": str(mx_action) if mx_action is not None else None,
        "built_at_distinct": int(built_at_distinct),   # must be 1 (single injected literal)
        "canonical_source_distribution": {k: int(v) for k, v in src_dist.items()},
        "canonical_source_bad_domain": int(bad_source_domain),
        "fresh_rows_with_enrichment": int(enrich_on_fresh),
        "columns": len(ds.schema.names),
        "indices": idx,
        "failures": failures,
        "pass": not failures,
    }
    return out


# =========================================================================================== #
# Modal entrypoint — the production substrate for the full build. The first full-build attempt on
# a 48 GiB / 313 GiB-free laptop died to session-coupled SIGKILL + unbounded spill; a Modal
# container is isolated (immune to harness/session restarts), sized for the index-stage external
# sort (LANCE_BYPASS_SPILLING ⇒ RAM-bound, flagged ≥96 GiB), and carries dedicated ephemeral NVMe
# for DuckDB spill + the local Lance stage. Zero new secrets/endpoints: existing named secrets
# only. The local CLI below remains the dev/smoke path — both wrap the same build()/index()/verify().
#   modal run    pipelines/usaspending/usaspending_fpds_canonical.py                      # build
#   modal run    pipelines/usaspending/usaspending_fpds_canonical.py --cmd index
#   modal run    pipelines/usaspending/usaspending_fpds_canonical.py --cmd verify
# =========================================================================================== #
if modal is not None:
    _image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("duckdb>=1.5,<2", "pylance>=7", "lancedb>=0.15", "pyarrow>=17",
                     "psycopg[binary]>=3.2", "boto3>=1.34")
        .env({
            # module-top constants read env at import — image env is the injection point.
            "FPDS_CANONICAL_DUCKDB_MEM": "160GB",
            "FPDS_CANONICAL_DUCKDB_THREADS": "16",
            "FPDS_CANONICAL_DUCKDB_TEMP_DIR": "/tmp/fpds_canonical_duckdb",
            "FPDS_CANONICAL_SCRATCH": "/tmp/fpds_canonical_stage",
        })
    )
    modal_app = modal.App("usaspending-fpds-canonical", image=_image)
    _SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]

    # NO auto-retries anywhere (pipeline discipline): a failed build is diagnosed, never re-fired
    # blind. ephemeral_disk expands /tmp → DuckDB spill + the local Lance stage both land on it.
    @modal_app.function(secrets=_SECRETS, timeout=60 * 60 * 12, memory=196_608, cpu=16.0,
                        ephemeral_disk=524_288, retries=0)  # 512 GiB — Modal's ephemeral_disk floor
    def build_fn(since: str | None = None, target_uri: str = CANONICAL_URI) -> dict:
        return build(since=since, target_uri=target_uri)

    # Index is now BOTH RAM- and disk-bound: RAM for the LANCE_BYPASS_SPILLING external sort (≥96 GiB
    # with headroom) AND ephemeral disk for the local dataset mirror (~90 GiB) + emitted index files,
    # since indices must be built locally then boto3-published (R2 rejects Lance's native multipart
    # index write). ephemeral_disk restored (#858 dropped it under the now-obsolete RAM-only model).
    @modal_app.function(secrets=_SECRETS, timeout=60 * 60 * 6, memory=196_608, cpu=8.0,
                        ephemeral_disk=524_288, retries=0)  # 512 GiB — Modal's ephemeral_disk floor
    def index_fn(target_uri: str = CANONICAL_URI) -> dict:
        return index(target_uri=target_uri)

    @modal_app.function(secrets=_SECRETS, timeout=60 * 45, memory=32_768, cpu=4.0, retries=0)
    def verify_fn(target_uri: str = CANONICAL_URI) -> dict:
        return verify(target_uri=target_uri)

    @modal_app.local_entrypoint()
    def modal_main(cmd: str = "build", since: str = "", target_uri: str = CANONICAL_URI):
        s = since or None
        if cmd == "build":
            print(json.dumps(build_fn.remote(since=s, target_uri=target_uri), indent=2, default=str))
        elif cmd == "build_spawn":
            # Fire-and-forget: submit build_fn + return immediately (client exits in seconds, so a
            # long-lived streaming client can't be killed mid-run). Pair with `modal run --detach` so
            # the app + spawned call survive the client exit. Poll R2/logs for the DONE state.
            call = build_fn.spawn(since=s, target_uri=target_uri)
            print(json.dumps({"spawned": "build_fn", "call_id": call.object_id,
                              "target_uri": target_uri, "since": s}, default=str))
        elif cmd == "index":
            print(json.dumps(index_fn.remote(target_uri=target_uri), indent=2, default=str))
        elif cmd == "index_spawn":
            # Fire-and-forget index (mirrors build_spawn): submit index_fn + return the call_id in
            # seconds, so a client capped well under the multi-hour index runtime cannot be killed
            # mid-build (the append-only publish lands only at the very end — a mid-run client death
            # would waste the whole sort). Pair with `modal run --detach` so the app + spawned call
            # survive the client exit; poll R2 list_indices() (or `modal app logs`) for the new index.
            call = index_fn.spawn(target_uri=target_uri)
            print(json.dumps({"spawned": "index_fn", "call_id": call.object_id,
                              "target_uri": target_uri}, default=str))
        elif cmd == "verify":
            print(json.dumps(verify_fn.remote(target_uri=target_uri), indent=2, default=str))
        else:
            raise SystemExit(f"unknown --cmd: {cmd} (build|build_spawn|index|index_spawn|verify)")


# =========================================================================================== #
# CLI
# =========================================================================================== #
def _arg_val(flag: str, argv: list[str], default: str | None = None) -> str | None:
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    return default


def main():
    argv = sys.argv[1:]
    cmd = argv[0] if argv else "verify"
    target_uri = _arg_val("--target-uri", argv, CANONICAL_URI)
    if cmd == "init_ops":
        init_ops()
    elif cmd == "build":
        since = _arg_val("--since", argv, None)
        print(json.dumps(build(since=since, target_uri=target_uri), indent=2, default=str))
    elif cmd == "index":
        print(json.dumps(index(target_uri=target_uri), indent=2, default=str))
    elif cmd == "verify":
        print(json.dumps(verify(target_uri=target_uri), indent=2, default=str))
    elif cmd == "print_merge_sql":
        # Inspection only — pure string, NO R2. Optional [--since DATE].
        # rejected: the fixed built_at literal here is intentional — this path never writes R2 and
        # exists only to dump the generated SQL for review; the real run injects the live naive-UTC
        # literal in build() (line 670-671). A run-date literal would add nondeterminism to inspection.
        since = _arg_val("--since", argv, None)
        print(_build_merge_sql(built_at_iso="2026-06-28 00:00:00.000000", since=since))
    else:
        print(f"unknown command: {cmd} (init_ops|build|index|verify|print_merge_sql)")
        sys.exit(2)


if __name__ == "__main__":
    main()
