"""USAspending FPDS CANONICAL transaction table (LOCAL CLI) — typed v2 SoR reconciliation.

Reconciles the THREE FPDS transaction feeds into ONE typed, PK-grained read model
(`s3://data-sink/active/usaspending_fpds_canonical_txn/`, Lance v2.1, ~78 typed columns):

    BULK   s3://data-sink/active/usaspending/transaction_search_fpds/   (~107.25M, 378 typed rpt.* cols)
    FRESH  s3://data-sink/active/usaspending_api_fresh/contract_prime_txn/ (~1.99M, 297 all-VARCHAR)
    ARCH_F s3://data-sink/active/usaspending_archive_full_fpds/         (~2.98M, 300 all-VARCHAR)
    ARCH_D s3://data-sink/active/usaspending_archive_delta_fpds/        (deletion ledger; correction_delete_ind)

CANONICAL VOCABULARY = the FPDS bulk_download/awards names (FRESH/archive carry them verbatim);
BULK is crosswalked into that vocabulary via the rpt.* map. BULK-only enrichment columns keep
their rpt.* names verbatim.

MERGE (TWO-TIER logical reconciliation; monthly-CSV corrections land + monthly-unique enrichment):
  • s()/kbulk() sentinel macros applied IDENTICALLY on every source — whole-string ''/'-NONE-' → NULL.
  • fresh_latest / bulk_latest / monthly_latest: EACH source collapsed to latest-per-key (deterministic
    tiebreaker). bulk_latest is ONE per-key collapse over FULL BULK (109M scanned ONCE). monthly_latest
    is collapsed over the FULL monthly projection (NOT an anti-joined survivor set), so monthly competes
    on shared keys — THE fix. (MONTHLY = the monthly bulk-download CSV feed; its physical R2 upstream is
    still named usaspending_archive_*_fpds — a tracked rename follow-up. In-code the semantic name is
    MONTHLY.)
  • TIER 1  bulk_base = bulk_latest ⊕ monthly_latest (LOGICAL CTE, NOT materialized): per-key
    argmax(last_modified_date); equal-mtime tie → MONTHLY wins over pg. The reconciled base is the
    semantic source of the enrichment fill.
  • TIER 2  canonical core = bulk_base ⊕ fresh_latest: per-key argmax; tie → FRESH. argmax is
    associative, so this two-tier order is executed as ONE flat 3-way row_number() window over the
    union of the three collapsed cores (source_rank FRESH<MONTHLY<BULK) — emitted CORE stays
    byte-identical to the pre-two-tier build (monthly.mtime ≥ pg on 100% of shared FY2026 keys).
  • Enrichment: pg-preferred fill from the reconciled base. 27 pg-only enrich cols = plain bulk_latest;
    12 monthly-unique cols (Treasury/federal-account funding + highly_compensated_officer_1..5 name +
    amount — pg LACKS all 12) = COALESCE(pg, monthly), monthly leg from a SEPARATE
    enrichment-populatedness dedup (monthly_enrich_latest). LEFT JOINs to PK-unique collapses → no
    fan-out. recipient_uei is CORE (argmax-resolved), NOT enrichment.
  • canonical_source: derived ONCE as the winning core row's src tag (fresh|bulk|monthly) — the true
    per-key winner, never a partition literal.
  • Tombstone (R6-scoped) − reinstatement (R5): delete_keys is scoped to the LATEST
    archive_snapshot_stamp (an old-month delete must not tombstone forever). A 'D' key is honored only
    when the reconciled winner mtime is NOT strictly newer than the delete mtime; a strictly-newer
    non-'D' row REINSTATES the key. One coupled final-state op applied to `resolved` (post fresh
    overlay). The delta scanner is filtered ONLY by correction_delete_ind='D' and NEVER receives --since.
  • Fail-closed PK-uniqueness gate raises BEFORE publish on any dup (structural: one survivor per key).

DISCIPLINES (d.8 / fleet rules):
  • module-top os.environ.setdefault("LANCE_BYPASS_SPILLING","true") BEFORE any import lance.
  • NO direct-R2 write of the table (Giants 400 InvalidPart) — LOCAL Lance write → boto3 uniform-part
    publish. data_storage_version="2.1", max_rows_per_file=1048576 (valid only on the boto3 path).
  • built_at = ONE Python naive-UTC literal injected into all three projections (NOT now()).
  • last_modified_date parsed via replace(...,'+00','')+TRY_CAST (NO strptime hard-abort).
  • NO auto-retries in pipeline logic; overwrite idempotency.
  • --since pushes action_date>= into the THREE DATA scanners ONLY (BULK date32; FRESH/archive
    lexical ISO-10 string), NEVER the delta scanner.

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

    # ---- (c2) MONTHLY-unique enrichment (canonical-vocab; pg/BULK LACKS all 12 → bulk_expr None =
    #   typed NULL on the BULK leg; monthly/archive is the SOLE populated source via feed_expr s()).
    #   COALESCE(pg, monthly) per key in the enrich block degenerates to monthly-only (pg absent),
    #   but the COALESCE form ships correctly and future-proofs a pg schema add. Names + TAS/federal-
    #   account lists stay VARCHAR; officer *_amount cols are typed DOUBLE (proven 0/507,542 non-castable). ----
    {"canonical": "treasury_accounts_funding_this_award", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(treasury_accounts_funding_this_award)"},
    {"canonical": "federal_accounts_funding_this_award", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(federal_accounts_funding_this_award)"},
    {"canonical": "highly_compensated_officer_1_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(highly_compensated_officer_1_name)"},
    {"canonical": "highly_compensated_officer_2_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(highly_compensated_officer_2_name)"},
    {"canonical": "highly_compensated_officer_3_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(highly_compensated_officer_3_name)"},
    {"canonical": "highly_compensated_officer_4_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(highly_compensated_officer_4_name)"},
    {"canonical": "highly_compensated_officer_5_name", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None, "feed_expr": "s(highly_compensated_officer_5_name)"},
    {"canonical": "highly_compensated_officer_1_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": "TRY_CAST(s(highly_compensated_officer_1_amount) AS DOUBLE)"},
    {"canonical": "highly_compensated_officer_2_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": "TRY_CAST(s(highly_compensated_officer_2_amount) AS DOUBLE)"},
    {"canonical": "highly_compensated_officer_3_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": "TRY_CAST(s(highly_compensated_officer_3_amount) AS DOUBLE)"},
    {"canonical": "highly_compensated_officer_4_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": "TRY_CAST(s(highly_compensated_officer_4_amount) AS DOUBLE)"},
    {"canonical": "highly_compensated_officer_5_amount", "duck_type": "DOUBLE", "group": "enrich",
     "bulk_expr": None, "feed_expr": "TRY_CAST(s(highly_compensated_officer_5_amount) AS DOUBLE)"},

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
              "federal_action_obligation", "recipient_hash", "award_id_piid"]
BITMAP_COLS = ["action_date_fiscal_year", "type_of_set_aside_code", "awarding_agency_code",
               "award_type_code", "idv_type_code", "canonical_source"]


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
            if tok in ("s", "kbulk", "TRY_CAST", "COALESCE", "AS", "DOUBLE", "BIGINT",
                       "DATE", "TIMESTAMP", "VARCHAR", "replace"):
                continue
            raw.add(tok)
    return sorted(raw)


# ----- canonical-vocabulary scanner column lists for the FRESH / archive feeds ----- #
def _feed_source_cols() -> list[str]:
    """Raw canonical-vocabulary columns the FRESH/archive projection reads (keys + core only;
    enrichment is BULK-only). Parsed from feed_expr."""
    import re
    raw: set[str] = set()
    for c in COLUMN_SPEC:
        expr = c["feed_expr"]
        if not expr:
            continue
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr):
            if tok in ("s", "TRY_CAST", "AS", "DOUBLE", "BIGINT", "DATE", "TIMESTAMP",
                       "VARCHAR", "replace"):
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

    Variant C — BRANCH per enrichment column by source availability:
      • pg-only enrich (feed_expr None; the 27 rpt.* cols): plain b.<col> from bulk_latest (pg). No
        monthly source exists → no COALESCE.
      • monthly-unique enrich (feed_expr set; the 12 TAS/federal + officer-comp cols): pg LACKS these
        canonical columns entirely, so the value is pg-preferred COALESCE(b.<col>, m.<col>) sourced
        from the reconciled bulk_base = pg⊕monthly. The pg leg (b) is a typed NULL placeholder for
        these 12 (bulk_expr None), so today the COALESCE degenerates to m.<col>; the form is kept so
        a future pg schema add is picked up automatically. m = monthly_enrich_latest (an
        enrichment-populatedness dedup, NOT monthly_latest's core dedup — see §3.6b)."""
    parts = []
    for c in _cols("enrich"):
        col = c["canonical"]
        if c["feed_expr"] is None:
            parts.append(f"    b.{col} AS {col}")
        else:
            parts.append(f"    COALESCE(b.{col}, m.{col}) AS {col}")
    return ",\n".join(parts)


def _stage1_sql(built_at_iso: str) -> str:
    """STAGE 1 — macros, the archive projection, and the three per-source collapses, with the
    bulk/fresh projections INLINED into their collapse windows (P1-4 spill hygiene: the ~107M-row
    bulk_proj duplicate materialization was the single largest spill contributor — 246 GB observed
    on-disk before the collapses even completed on the first full-build attempt). archive_proj
    stays materialized: it is small (~3M rows) and legitimately read twice (§3.4 core dedup +
    §3.6b enrichment dedup). Executed as ONE multi-statement script; the schema-identity gate then
    runs against the three COLLAPSED tables (identical canonical NAME+ORDER+TYPE by construction —
    collapse = SELECT * EXCLUDE (rn) over a projection-shaped inner) before the stage-2 merge.

    Also captured here, as 1-row m_* tables, every metric whose source table is dropped in stage 2
    (free-as-you-go DROPs), plus the narrow bulk_keys set (1 col) that outlives bulk_latest for the
    late monthly_corrections metric."""
    bulk_proj = _proj_select("bulk", built_at_iso)
    fresh_proj = _proj_select("feed", built_at_iso)
    arch_proj = _proj_select("feed", built_at_iso)
    return f"""{_MACROS}
-- ===== §3.1 archive projection (materialized ONCE; read twice: §3.4 + §3.6b) ===== --
CREATE TEMP TABLE archive_proj AS
SELECT
{arch_proj}
FROM archive_r;

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

-- ===== §3.4 MONTHLY collapse → latest-per-key over the FULL monthly projection (THE fix) ===== --
-- NOTE: the physical R2 upstream is still usaspending_archive_full_fpds (registered as archive_r →
-- archive_proj); renaming that dataset is a tracked follow-up. In-code the SEMANTIC name is MONTHLY
-- (the monthly bulk-download CSV feed). Built over the ENTIRE monthly projection (NOT an anti-joined
-- survivor set), so monthly competes on shared keys. monthly lacks a stable transaction surrogate →
-- contract_award_unique_key (the same surrogate fresh_latest uses). monthly_full is FY2026-only
-- (2025-10-01..2026-06-04): under --since 2025-10-01 monthly_latest is the COMPLETE monthly universe
-- = complete correction-proof scope. CORE dedup ordering stays core-populatedness/mtime (do NOT
-- switch to enrichment-populatedness here — that would perturb the core argmax; §3.6b handles the
-- enrichment-first monthly row SEPARATELY).
CREATE TEMP TABLE monthly_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY contract_transaction_unique_key
            ORDER BY last_modified_date DESC NULLS LAST,
                     (federal_action_obligation IS NULL) ASC,
                     contract_award_unique_key DESC NULLS LAST) AS rn
  FROM archive_proj
  WHERE contract_transaction_unique_key IS NOT NULL
) WHERE rn = 1;

-- ===== §3.4k narrow BULK key set (1 col) — outlives bulk_latest for late metrics ===== --
CREATE TEMP TABLE bulk_keys AS
SELECT contract_transaction_unique_key AS k FROM bulk_latest;

-- ===== early metric captures (1-row each) — sources dropped at stage-2 boundaries ===== --
CREATE TEMP TABLE m_rows_in_fresh AS SELECT count(*) AS c FROM fresh_r;
CREATE TEMP TABLE m_rows_in_archive AS SELECT count(*) AS c FROM archive_proj;
-- BULK is consumed exactly once by the inlined collapse (single-pass reader) — the raw scanned
-- count cannot be re-taken. BULK is proven PK-unique (107,250,527 distinct == rowcount), so the
-- post-collapse count equals rows scanned; if BULK ever grows dup keys, dedup_collapsed
-- undercounts by exactly those dups (documented, not silent).
CREATE TEMP TABLE m_rows_in_bulk AS SELECT count(*) AS c FROM bulk_latest;
CREATE TEMP TABLE m_fresh_only_tail AS
SELECT count(*) AS c FROM fresh_latest f
ANTI JOIN bulk_keys b ON f.contract_transaction_unique_key = b.k;
"""


DELTA_STAMP_COL = "archive_snapshot_stamp"


def _stage2_sql(delta_has_stamp: bool = True) -> str:
    """STAGE 2 — the merge: TWO-TIER logical reconciliation, ONE physical artifact. Pipeline:
      (stage-1 collapses: fresh_latest / bulk_latest / monthly_latest, ≤1 row per key each)
        → bulk_base    (Tier 1, LOGICAL CTE — NOT materialized as a separate artifact: pg⊕monthly
                        per-key argmax(last_modified_date); equal-mtime tie → monthly WINS over pg)
        → core_union   (UNION ALL BY NAME of the three collapsed CORES, each tagged src+source_rank)
        → core_winner  (Tier 2, SINGLE 3-way window: argmax(last_modified_date) per key, total-order
                        tiebreak — associativity-equivalent to (bulk_base ⊕ fresh_latest); tie→FRESH)
        → monthly_enrich_latest (§3.6b enrichment-populatedness dedup — the monthly leg of the fill)
        → resolved     (LEFT JOIN bulk_latest [pg] + LEFT JOIN monthly_enrich_latest [monthly] →
                        branched enrichment REPLACE [COALESCE pg-preferred for the 12 monthly-unique
                        cols, plain pg for the 27] + w.src AS canonical_source)
        → canonical_out(R6-scoped tombstone with R5 reinstatement + locked canonical projection)

    P1-4 SPILL HYGIENE: every giant TEMP TABLE is DROPped at its last-reader boundary (free-as-you-go),
    and the metrics whose sources are dropped are captured first as 1-row m_* tables (stage 1 +
    m_merged / m_deletes / m_monthly_corr here). Peak concurrent spill is bounded by the join inputs
    of the widest single statement (~3 wide ~107M-row relations at `resolved`), not by the sum of
    every intermediate (~6 wide tables ≈ 350-520 GB unbounded, the first-attempt failure mode).

    TWO-TIER INVARIANT (CORE byte-identity): the emitted CORE is argmax(last_modified_date) over
    {FRESH, BULK, MONTHLY}. argmax is associative, so the explicit two-tier framing
    (bulk_base = BULK⊕MONTHLY, then canonical = bulk_base⊕FRESH) is IDENTICAL to the flat 3-way
    window kept below. The tier-1 equal-mtime tie (monthly>pg) is subsumed by source_rank
    (MONTHLY=2 < BULK=3) in the flat window; the tier-2 tie (fresh) by FRESH=1. Proven on the
    --since 2025-10-01 window: monthly.mtime ≥ pg on 100% of 2,189,379 shared FY2026 keys (0 older),
    so the flat window emits byte-identical CORE. bulk_base is retained as a documented LOGICAL CTE
    so the reconciled base is the semantic source of the enrichment fill; it is NOT a second physical
    Lance dataset — the single artifact usaspending_fpds_canonical_txn/ is unchanged.

    THE FIX (correction landing): MONTHLY competes for the volatile core on EVERY key (monthly_latest
    is collapsed over the FULL monthly projection, not an anti-joined survivor set), so a
    strictly-newer monthly correction lands for keys shared with BULK/FRESH. PK-uniqueness is
    structural (row_number()=1 over ≤1-per-source collapses). Pure string; references the stage-1
    collapses + archive_delta_D relation. Executed as ONE multi-statement script."""
    enrich_block = _enrich_replace_block()
    canon_cols = ", ".join(_canon_order())
    # R6 stamp scoping fragments — only when the delta feed actually carries the stamp column.
    if delta_has_stamp:
        latest_stamp_expr = f"max({DELTA_STAMP_COL}) AS s"
        stamp_predicate = (f", latest WHERE {DELTA_STAMP_COL} = latest.s "
                           f"OR (latest.s IS NULL AND {DELTA_STAMP_COL} IS NULL)")
    else:
        latest_stamp_expr = "CAST(NULL AS VARCHAR) AS s"
        stamp_predicate = ""
    return f"""-- ===== §3.4b TIER-1 bulk_base = bulk_latest ⊕ monthly_latest (LOGICAL CTE, NOT materialized) ===== --
-- Explicit two-tier framing: reconcile pg (bulk) with monthly by per-key argmax(last_modified_date);
-- equal-mtime tie → MONTHLY WINS over pg (rank 2 < 3). This is the semantic "reconciled base" the
-- enrichment fill (§3.7) draws from. It is a documented VIEW, not a second Lance artifact; the CORE
-- values it would emit are subsumed by the flat 3-way core_winner below (argmax associativity), so
-- it is defined for clarity/traceability and to name the tier boundary — the flat window remains the
-- executed path, keeping emitted CORE byte-identical to the pre-two-tier build.
CREATE TEMP VIEW bulk_base AS
SELECT * EXCLUDE (src, source_rank, rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY contract_transaction_unique_key
            ORDER BY last_modified_date DESC NULLS LAST, source_rank ASC,
                     contract_award_unique_key DESC NULLS LAST) AS rn
  FROM (
    SELECT CAST('monthly' AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, m.* FROM monthly_latest m
    UNION ALL BY NAME
    SELECT CAST('bulk'    AS VARCHAR) AS src, CAST(3 AS INTEGER) AS source_rank, b.* FROM bulk_latest b
  )
) WHERE rn = 1;

-- ===== §3.5 three collapsed CORES → vertical union, each tagged src + source_rank ===== --
-- src CAST identically as VARCHAR and source_rank as INTEGER in all three arms so BY-NAME union
-- types align. source_rank encodes the locked precedence FRESH(1) > MONTHLY(2) > BULK(3) — which is
-- exactly the two-tier order flattened: tier-2 tie→FRESH (rank 1), tier-1 tie→MONTHLY (rank 2 < 3).
CREATE TEMP TABLE core_union AS
SELECT CAST('fresh'   AS VARCHAR) AS src, CAST(1 AS INTEGER) AS source_rank, f.* FROM fresh_latest f
UNION ALL BY NAME
SELECT CAST('monthly' AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, a.* FROM monthly_latest a
UNION ALL BY NAME
SELECT CAST('bulk'    AS VARCHAR) AS src, CAST(3 AS INTEGER) AS source_rank, b.* FROM bulk_latest b;

-- P1-4 boundary: core_union was the last reader of fresh_latest and monthly_latest (their metrics
-- were captured in stage 1). bulk_base is documentation-only (never queried) and must go before its
-- base tables. bulk_latest LIVES ON (enrichment source at §3.7).
DROP VIEW bulk_base;
DROP TABLE fresh_latest;
DROP TABLE monthly_latest;

-- ===== §3.6 SINGLE 3-way per-key core resolution: argmax(last_modified_date) (= flat two-tier) ===== --
-- Provably total order: after the three upstream collapses there is AT MOST one row per source per
-- key, so source_rank alone disambiguates every cross-source mtime tie; the trailing award-key term
-- is defense-in-depth. NULL mtime sorts LAST (= oldest) per BLOCKER-1. Exactly one survivor per key
-- (row_number()=1) → PK-uniqueness is structural, not anti-join-disjointness-dependent. This flat
-- window is argmax-associativity-identical to Tier2(bulk_base ⊕ fresh_latest); see §3.4b.
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

-- ===== §3.6b monthly ENRICHMENT-populatedness dedup (the monthly leg of the COALESCE) ===== --
-- SEPARATE from monthly_latest's CORE dedup (§3.4): that one ranks on core-populatedness/mtime and
-- can surface a latest-but-enrich-NULL row, forfeiting the gain. Here rank ENRICHMENT-populated rows
-- ABOVE enrich-NULL rows for the same key, then latest-mtime among equally-populated, then a stable
-- award-key surrogate. On the --since 2025-10-01 window the delta vs the latest-mtime dedup is
-- empirically 0 (every key's latest-mtime monthly row already carries its enrichment), so CORE stays
-- byte-identical; it is REQUIRED as a cadence-robustness safeguard for a future core-only monthly
-- re-dump that lands a newer enrich-NULL row. Keyed downstream on the txn key → ≤1 row per key.
CREATE TEMP TABLE monthly_enrich_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY contract_transaction_unique_key
            ORDER BY (treasury_accounts_funding_this_award IS NULL
                      AND federal_accounts_funding_this_award IS NULL
                      AND highly_compensated_officer_1_name IS NULL) ASC,
                     last_modified_date DESC NULLS LAST,
                     contract_award_unique_key DESC NULLS LAST) AS rn
  FROM archive_proj
  WHERE contract_transaction_unique_key IS NOT NULL
) WHERE rn = 1;

-- P1-4 boundary: monthly_enrich_latest was archive_proj's second and last reader.
DROP TABLE archive_proj;

-- ===== §3.7 enrichment fill: pg (bulk_latest) + monthly (monthly_enrich_latest) ===== --
-- Both LEFT JOINs are to PK-unique per-key collapses → no fan-out. The enrichment REPLACE (§3.7
-- builder) overwrites the enrich half: plain b.<col> for the 27 pg-only cols; pg-preferred
-- COALESCE(b.<col>, m.<col>) for the 12 monthly-unique cols (pg is a typed-NULL placeholder for
-- those, so today COALESCE = m.<col>; the reconciled bulk_base semantics = pg-preferred fill). b is
-- NULL for archive-only/fresh-only keys; m is NULL for fresh-only/pg-only keys. canonical_source is
-- derived HERE, exactly once, as the winning core row's src tag (INV-7) — the true per-key winner
-- (fresh|bulk|monthly), never a partition literal.
-- EXCLUDE the placeholder canonical_source carried up from the projections (typed NULL) as well as
-- src, then re-derive canonical_source := w.src. Excluding the placeholder is REQUIRED: keeping it
-- would collide with `w.src AS canonical_source` and DuckDB would silently rename the derived column
-- (canonical_source_1), leaving the locked-order projection to read the all-NULL placeholder.
CREATE TEMP TABLE resolved AS
SELECT
  w.* EXCLUDE (src, canonical_source) REPLACE (
{enrich_block}
  ),
  w.src AS canonical_source
FROM core_winner w
LEFT JOIN bulk_latest b ON w.contract_transaction_unique_key = b.contract_transaction_unique_key
LEFT JOIN monthly_enrich_latest m ON w.contract_transaction_unique_key = m.contract_transaction_unique_key;

-- P1-4 boundary: resolved supersedes core_winner + both enrichment legs. bulk_latest's late metric
-- (monthly_corrections) reads the narrow bulk_keys captured in stage 1, so the 107M-wide table goes
-- here. rows_in_bulk was captured in stage 1 (m_rows_in_bulk).
DROP TABLE core_winner;
DROP TABLE monthly_enrich_latest;
DROP TABLE bulk_latest;

-- ===== §3.8 tombstone (R6-scoped) − reinstatement (R5) → canonical_out ===== --
-- delta scanner is filtered ONLY by correction_delete_ind='D' (caller side); NEVER --since.
-- R6 SNAPSHOT-STAMP SCOPING: delta 'D' rows accumulate across monthly snapshots; an OLD-month delete
-- must NOT tombstone forever. Scope delete_keys to the LATEST archive_snapshot_stamp present in the
-- delta-'D' set (a single scalar), so only the current snapshot's deletes apply. Guarded by
-- has_stamp: if the delta feed lacks the stamp column, fall back to the whole 'D' set (no scoping).
CREATE TEMP TABLE delete_keys AS
WITH d AS (SELECT * FROM archive_delta_D WHERE s(contract_transaction_unique_key) IS NOT NULL),
     latest AS (SELECT {latest_stamp_expr} FROM d)
SELECT s(contract_transaction_unique_key) AS k,
       max(TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP)) AS delta_lmt
FROM d {stamp_predicate}
GROUP BY 1;

-- R5 REINSTATEMENT GATE: a delete tombstone is honored ONLY when the WINNING RECONCILED-winner row
-- (post Tier-2, in `resolved`) is NOT strictly newer than the delete's last_modified_date. If the
-- reconciled winner mtime > delta_lmt, the key was RE-INSTATED by a newer (non-'D') source row after
-- the delete — do NOT tombstone it. Ground fact (--since 2025-10-01): 92/656 'D' keys are live in
-- monthly_full; 39 are strictly-newer → those 39 survive. Tombstone-minus-reinstatement is ONE
-- coupled final-state op applied AFTER the fresh overlay (to `resolved`, never to the base alone).
-- Implemented as a LEFT JOIN + WHERE: keep a row unless it matches a delete that is NOT reinstated.
CREATE TEMP TABLE canonical_out AS
SELECT {canon_cols} FROM resolved
LEFT JOIN delete_keys d ON resolved.contract_transaction_unique_key = d.k
WHERE d.k IS NULL
   OR (resolved.last_modified_date IS NOT NULL AND resolved.last_modified_date > d.delta_lmt);

-- ===== late metric captures, then the last P1-4 boundary ===== --
-- deletes_tombstoned = delete-keys present in resolved that were ACTUALLY dropped — i.e. NOT
-- R5-reinstated (the complement of the reinstatement predicate; post-R5 truth, not raw 'D' matches).
CREATE TEMP TABLE m_deletes AS
SELECT count(DISTINCT r.contract_transaction_unique_key) AS c FROM resolved r
JOIN delete_keys d ON r.contract_transaction_unique_key = d.k
WHERE r.last_modified_date IS NULL OR r.last_modified_date <= d.delta_lmt;
-- monthly_corrections_applied = canonical keys the MONTHLY core WON over a key BULK also holds —
-- the true correction count. SEMI JOIN on the narrow bulk_keys (bulk_latest already dropped).
CREATE TEMP TABLE m_monthly_corr AS
SELECT count(*) AS c FROM canonical_out co
SEMI JOIN bulk_keys b ON co.contract_transaction_unique_key = b.k
WHERE co.canonical_source = 'monthly';

DROP TABLE resolved;
DROP TABLE delete_keys;
DROP TABLE bulk_keys;
"""


def _build_merge_sql(*, built_at_iso: str, since: str | None) -> str:
    """The FULL merge SQL (stage 1 + stage 2) concatenated — for inspection / print_merge_sql ONLY.
    build() executes _stage1_sql() and _stage2_sql() separately so the schema-identity gate can run
    between them (against the collapsed tables). No R2 access here; safe to print.

    --since note: the predicate is pushed into the THREE DATA scanners (caller/build side), NEVER the
    delta scanner. Carried here only as a comment marker for traceability."""
    since_note = (f"-- --since={since} pushed into the THREE data scanners only "
                  f"(delta NEVER filtered)\n" if since else "")
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
    """§4 enforcement — PROGRAMMATIC, not aspirational. The three per-source COLLAPSES (which are
    SELECT * EXCLUDE (rn) over projection-shaped inners, so they carry the projections' exact
    (name, type) sequences) MUST be byte-identical before any union. Raise on mismatch (hard build
    failure rather than a silent transposition). P1-4 note: the gate moved from the *_proj trio to
    the collapsed tables because the bulk/fresh projections are inlined and never materialized."""
    sigs = {}
    for name in ("bulk_latest", "fresh_latest", "monthly_latest"):
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
    con = None
    local_ds = os.path.join(SCRATCH, "canonical_lance")
    try:
        os.makedirs(SCRATCH, exist_ok=True)
        shutil.rmtree(local_ds, ignore_errors=True)

        # --since pushed into the THREE DATA scanners ONLY. BULK action_date is date32 → compare to a
        # DATE literal; FRESH/archive action_date is lexical ISO-10 string (0 nulls) → lexical compare.
        bulk_data_filter = f"action_date >= DATE '{since}'" if since else None
        feed_data_filter = f"action_date >= '{since}'" if since else None

        bulk_ds = lance.dataset(BULK_URI, storage_options=so)
        fresh_ds = lance.dataset(FRESH_URI, storage_options=so)
        arch_ds = lance.dataset(ARCHIVE_FULL_URI, storage_options=so)
        delta_ds = lance.dataset(ARCHIVE_DELTA_URI, storage_options=so)

        bulk_present = set(bulk_ds.schema.names)
        feed_keys_core = _feed_source_cols()
        fresh_present = set(fresh_ds.schema.names)
        arch_present = set(arch_ds.schema.names)
        delta_present = set(delta_ds.schema.names)

        bulk_scan_cols = [c for c in _bulk_source_cols() if c in bulk_present]
        fresh_scan_cols = [c for c in feed_keys_core if c in fresh_present]
        arch_scan_cols = [c for c in feed_keys_core if c in arch_present]
        # R6: archive_snapshot_stamp scopes delete_keys to the LATEST monthly snapshot. Include it in
        # the delta scan when present; delta_has_stamp gates the scoping fragments in _merge_tail_sql.
        delta_scan_cols = [c for c in ("contract_transaction_unique_key", "last_modified_date",
                                       "correction_delete_ind", DELTA_STAMP_COL) if c in delta_present]
        delta_has_stamp = DELTA_STAMP_COL in delta_present

        con = _duck()
        log(f"registering sources (since={since}) → target {target_uri}")
        # BULK: single pass into the per-key collapse → .to_reader().
        con.register("bulk_r", bulk_ds.scanner(columns=bulk_scan_cols,
                                               filter=bulk_data_filter).to_reader())
        # FRESH / archive: deduped then probed (multi-pass) → .to_table() (re-scannable).
        con.register("fresh_r", fresh_ds.scanner(columns=fresh_scan_cols,
                                                 filter=feed_data_filter).to_table())
        con.register("archive_r", arch_ds.scanner(columns=arch_scan_cols,
                                                  filter=feed_data_filter).to_table())
        # delta: FIXED/EXCLUSIVE correction_delete_ind='D'; NEVER --since.
        con.register("archive_delta_D", delta_ds.scanner(
            columns=delta_scan_cols, filter="correction_delete_ind = 'D'").to_table())

        # Stage 1: archive projection + the three collapses (bulk/fresh projections INLINED — P1-4),
        # then ENFORCE schema identity on the collapsed tables before any union (§4 — programmatic
        # gate). Stage 2: the merge with free-as-you-go DROPs; metrics whose sources are dropped
        # were captured as 1-row m_* tables at the correct boundaries.
        con.execute(_stage1_sql(built_at_iso))
        _assert_collapse_schema_identity(con)
        con.execute(_stage2_sql(delta_has_stamp=delta_has_stamp))

        # rows_in_bulk = post-collapse count (BULK proven PK-unique → equals rows scanned; the
        # single-pass reader cannot be re-counted after the inlined collapse — see _stage1_sql).
        rows_in_bulk = con.execute("SELECT c FROM m_rows_in_bulk").fetchone()[0]
        rows_in_fresh = con.execute("SELECT c FROM m_rows_in_fresh").fetchone()[0]
        rows_in_archive_full = con.execute("SELECT c FROM m_rows_in_archive").fetchone()[0]

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
        deletes_tombstoned = con.execute("SELECT c FROM m_deletes").fetchone()[0]
        # rows_out INVARIANT: distinct-key count of core_union = |FRESH∪BULK∪MONTHLY keys|; landing a
        # correction REPLACES a key's core, never adds/removes a key. core_winner had exactly that
        # count (one survivor per key) — captured as m_merged before its drop.
        merged_rows = con.execute("SELECT c FROM m_merged").fetchone()[0]
        dedup_collapsed = int(rows_in_bulk + rows_in_fresh + rows_in_archive_full - merged_rows)
        monthly_corrections_applied = con.execute("SELECT c FROM m_monthly_corr").fetchone()[0]
        max_action_date = con.execute("SELECT max(action_date) FROM canonical_out").fetchone()[0]

        log(f"core_winner={merged_rows:,} rows_out={rows_out:,} fresh_only_tail={fresh_only_tail:,} "
            f"deletes_tombstoned={deletes_tombstoned:,} "
            f"monthly_corrections_applied={monthly_corrections_applied:,} "
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

        s3 = _s3()
        log(f"publishing local Lance → {target_uri} (boto3 uniform-part)…")
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
                    indices_built=None, status=status, error=error,
                    started=started, completed=dt.datetime.now(dt.timezone.utc))
        shutil.rmtree(SCRATCH, ignore_errors=True)
    return metrics


def index(target_uri: str = CANONICAL_URI) -> dict:
    """Open the published dataset and build the §4 BTREE/BITMAP indices. Separate from build so a
    failed/half index never corrupts the data write. Columns absent from schema are skipped."""
    import lance
    so = _r2_so()
    ds = lance.dataset(target_uri, storage_options=so)
    present = set(ds.schema.names)
    built: list[str] = []
    log(f"indexing {target_uri} ({ds.count_rows():,} rows)")
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
    log(f"indices built: {built}")
    return {"target_uri": target_uri, "indices_built": built}


def verify(target_uri: str = CANONICAL_URI) -> dict:
    """§5/§7 assertions on read-back. Independent scanner → DuckDB. Set-membership via ANTI JOIN
    (never NOT IN — NULL-poison). Returns JSON with a `pass` verdict and a `failures` list.

    §7 RE-BASELINED CENTERLINES (two-tier reconciliation; monthly corrections land + enrichment fill).
    Absolute row counts scale with --since, so the GATED assertions are scope-independent
    structural invariants; the absolute numbers below are recorded as the full-build (--since NULL)
    reference and the --since 2025-10-01 proof scope, not hard-coded gate thresholds.

      metric                        full-build reference     --since 2025-10-01 (proof)
      rows_out                      ≈ |FRESH∪BULK∪MONTHLY| − tombstones + reinstatements
      fresh_only_tail (keys∉BULK)   ≈ FRESH-only key count                        (scope-dependent)
      deletes_tombstoned            present-in-universe D-keys, POST R5 reinstatement (39-key floor
                                    survives on the --since 2025-10-01 window)
      monthly_corrections_applied   ≥ monthly-only + shared monthly-wins
                                    (monthly is the per-key core winner; > 0 REQUIRED)
      canonical_source ∈ {fresh,bulk,monthly}; BULK dethroned on every shared key where a newer
        (or equal-mtime, monthly-ranked-higher) source exists.

    GATES (raise / verdict=fail): INV-1 PK-unique; INV-4 rows_out == distinct-key count;
    built_at_distinct == 1; canonical_source domain ⊆ {fresh,bulk,monthly}; INV-7 zero monthly-labeled
    keys with a strictly-newer present fresh/bulk mtime (cannot check cross-source on the read model →
    surfaced as canonical_source_bad_domain only); monthly_corrections_applied > 0."""
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
    # §7 monthly-corrections centerline: monthly is the per-key CORE winner (INV-3 fix landed).
    monthly_corrections_applied = con.execute(
        "SELECT count(*) FROM c WHERE canonical_source = 'monthly'").fetchone()[0]
    # canonical_source domain gate (INV-7): only the three legal winner tags may appear.
    bad_source_domain = con.execute(
        "SELECT count(*) FROM c WHERE canonical_source IS NULL "
        "OR canonical_source NOT IN ('fresh','bulk','monthly')").fetchone()[0]
    con.close()

    failures: list[str] = []
    if total != distinct:
        failures.append(f"INV-1/INV-4 PK-unique+rows_out: count(*)={total:,} != "
                        f"distinct_key={distinct:,} ({total - distinct:,} dup keys)")
    if built_at_distinct != 1:
        failures.append(f"built_at not a single literal: built_at_distinct={built_at_distinct}")
    if bad_source_domain:
        failures.append(f"INV-7 canonical_source domain: {bad_source_domain:,} rows NULL or "
                        f"∉ {{fresh,bulk,monthly}}")
    if monthly_corrections_applied <= 0:
        failures.append("monthly_corrections_applied == 0 — corrections did NOT land "
                        "(monthly never won a core; the P0-1 fix regressed)")

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
        "monthly_corrections_applied": int(monthly_corrections_applied),
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

    # Index = the RAM-bound external sort (LANCE_BYPASS_SPILLING) — sized ≥96 GiB with headroom.
    # No ephemeral_disk: its pressure is RAM, not local disk (Modal's floor is 512 GiB — wasteful here).
    @modal_app.function(secrets=_SECRETS, timeout=60 * 60 * 6, memory=196_608, cpu=8.0,
                        retries=0)
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
        elif cmd == "index":
            print(json.dumps(index_fn.remote(target_uri=target_uri), indent=2, default=str))
        elif cmd == "verify":
            print(json.dumps(verify_fn.remote(target_uri=target_uri), indent=2, default=str))
        else:
            raise SystemExit(f"unknown --cmd: {cmd} (build|index|verify)")


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
