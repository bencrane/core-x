"""USAspending SUBAWARD CANONICAL table (LOCAL CLI) — typed v2 SoR reconciliation.

Reconciles the TWO contract-subaward feeds into ONE typed, composite-PK-grained read model
(`s3://data-sink/active/usaspending_subaward_canonical/`, Lance v2.1, ~91 typed columns):

    BULK   s3://data-sink/active/usaspending/subaward_search/            (9.80M rows, 210 typed cols;
             contract-only subset prime_award_group='procurement' = 2,643,501 rows)
    FRESH  s3://data-sink/active/usaspending_api_fresh/contract_subaward/ (321,204 rows, 118 all-VARCHAR,
             procurement-only by construction — the accumulating FSRS daily-append overlay)

This is the SUBAWARD counterpart of usaspending_fpds_canonical.py (the prime spine). A subaward is a
CHILD of a prime award (up to ~23k subawards per prime) → SEPARATE canonical, never folded into the
prime PK-grained table. Scope is CONTRACT-ONLY (mirrors FRESH); grant subawards (7.16M, BULK-only) are a
separate future canonical.

CANONICAL VOCABULARY = the FRESH/bulk_download subaward names (subawardee_uei, prime_award_unique_key,
subaward_number, …); BULK is crosswalked into that vocabulary via the rpt.* map. BULK-only / FRESH-only
enrichment columns keep their native names.

RECONCILIATION (TWO-SOURCE per-key argmax — simpler than the prime spine's 4-feed two-tier):
  • Composite PK: (prime_award_unique_key, subaward_number). Proven by the reconciliation probe:
    90.27% FRESH containment in BULK, 14,322 FRESH-only tail, rows_out centerline ≈ 1.32M. A synthesized
    single-column key `subaward_unique_key` = prime_award_unique_key|subaward_number is carried for BTREE
    point-lookup; the fail-closed gate uses the TUPLE, never the string.
  • Grain: ONE row per composite. Native PKs (broker_subaward_id / subaward_sam_report_id) are the
    PRE-collapse report grain (~2.0 BULK / ~2.18 FRESH rows per composite; FRESH also carries 94,276 dup
    subaward_sam_report_id rows from its daily re-pull). Each source is collapsed latest-per-composite.
  • s() sentinel macro applied IDENTICALLY on both sources — whole-string ''/'-NONE-' → NULL.
  • fresh_latest / bulk_latest: EACH source collapsed to latest-per-composite (row_number()=1,
    ORDER subaward_last_modified_date DESC NULLS LAST, <source surrogate> DESC). PK-uniqueness is
    STRUCTURAL (one survivor per composite per source), not disjointness-dependent.
  • core_winner: ONE flat 2-way window over the union of the two collapsed cores (source_rank
    FRESH(1) < BULK(2)) → argmax(subaward_last_modified_date); tie → FRESH. subaward_last_modified_date is
    the unified mod-frontier (BULK broker_updated_at vs FRESH SAM-report last-modified). Cross-clock:
    FRESH generally ≥ BULK, and tie→FRESH, so FRESH wins the recent overlay exactly as intended.
  • Enrichment (SINGLE-SOURCE per column, independent of the core winner): BULK-only enrich cols filled
    from bulk_latest (b.<col>); FRESH-only enrich cols filled from fresh_latest (f.<col>). LEFT JOINs to
    PK-unique collapses → no fan-out. No COALESCE (each enrich has exactly one source).
  • canonical_source: derived ONCE as the winning core row's src tag (fresh|bulk) — the true per-key
    winner, never a partition literal.
  • NO monthly feed, NO deletion ledger, NO tombstone/reinstatement (the prime spine's R5/R6). FRESH is
    accumulating-append, BULK is a periodic dump; a subaward is never "deleted", only superseded.
  • Fail-closed PK-uniqueness gate raises BEFORE publish on any dup (structural: one survivor per
    composite) + a synthesized-key collision gate (subaward_unique_key distinct == composite distinct).

DISCIPLINES (d.8 / fleet rules):
  • module-top os.environ.setdefault("LANCE_BYPASS_SPILLING","true") BEFORE any import lance.
  • DIRECT-R2 write + DIRECT-R2 index. At ~1.3M rows the dataset is FAR under the ~100M "giant" threshold
    that forces the prime spine onto local-stage → boto3 uniform-part publish; direct-R2 is the sanctioned
    non-giant default (proven by contractor_award_summary.py). data_storage_version="2.1".
  • built_at = ONE Python naive-UTC literal injected into both projections (NOT now()).
  • subaward_last_modified_date parsed via replace(...,'+00','')+TRY_CAST (NO strptime hard-abort).
  • FSRS source-data quality: subaward_amount carries a 1.0e18 sentinel; subaward_action_date carries
    1900/2106 sentinels. Carried RAW on the spine (faithful SoR); the ledger max-date metric is clamped to
    [.., CURRENT_DATE] for sanity and consumers clamp for display (mirror contractor_award_summary).
  • NO auto-retries in pipeline logic; overwrite idempotency.
  • --since pushes subaward_action_date>= into BOTH data scanners (BULK date32; FRESH lexical ISO-10).

    # FULL contract-only build (runs ON-BOX — ~1.3M rows, no Modal required for correctness):
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'psycopg[binary]>=3.2' \
      python3 -m pipelines.usaspending.usaspending_subaward_canonical build   # then: index ; then: verify

    # SAMPLE (small slice):
    doppler run -p core-x -c prd -- uv run --no-project ... \
      python3 -m pipelines.usaspending.usaspending_subaward_canonical build --since 2025-10-01 \
        --target-uri s3://data-sink/active/_sample/usaspending_subaward_canonical_sample/

    python3 -m pipelines.usaspending.usaspending_subaward_canonical init_ops
    python3 -m pipelines.usaspending.usaspending_subaward_canonical index  [--target-uri URI]
    python3 -m pipelines.usaspending.usaspending_subaward_canonical verify [--target-uri URI]
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path

# In-RAM scalar-index sort (ARCHITECTURE.md fleet rule). Set BEFORE any lance call.
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

try:  # Modal is the production/cadence substrate; the local CLI is the dev + on-box full-build path.
    import modal
except ImportError:
    modal = None

BUCKET = "data-sink"
ACTIVE = "s3://data-sink/active"

BULK_URI = f"{ACTIVE}/usaspending/subaward_search/"
FRESH_URI = f"{ACTIVE}/usaspending_api_fresh/contract_subaward/"
CANONICAL_URI = f"{ACTIVE}/usaspending_subaward_canonical/"

# Contract-only scope (mirrors FRESH's procurement-only construction). Pushed into the BULK scanner.
CONTRACT_FILTER = "prime_award_group = 'procurement'"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576
MAX_BYTES_PER_FILE = 90 * 1024**3

DUCK_MEM = os.environ.get("SUBAWARD_CANONICAL_DUCKDB_MEM", "8GB")
DUCK_TMP = os.environ.get("SUBAWARD_CANONICAL_DUCKDB_TEMP_DIR", "/tmp/subaward_canonical_duckdb")
DUCK_THREADS = int(os.environ.get("SUBAWARD_CANONICAL_DUCKDB_THREADS", "6"))

FEED = "usaspending_subaward_canonical"
OPS_SQL_FILE = "ops_usaspending_subaward_canonical_runs.sql"
OPS_TABLE = "ops.usaspending_subaward_canonical_runs"


# =========================================================================================== #
# THE COLUMN CONTRACT — the SINGLE source of truth. Every projection, the enrichment REPLACE
# block, the final column order, and the index lists are PROGRAM-GENERATED from this structure.
# Locked by the Data-Dictionary column-selection pass (see docs/reference/SUBAWARD_CANONICAL_*).
#
# Each entry:
#   canonical : output column name (= FRESH/bulk_download subaward vocabulary)
#   duck_type : DuckDB target type (DATE→date32, TIMESTAMP→naive timestamp[us], DOUBLE, BIGINT, VARCHAR)
#   group     : 'key' | 'core' | 'enrich' | 'prov'
#   bulk_expr : BULK subaward_search projection expr (rpt.* crosswalk). None ⇒ typed-NULL placeholder.
#   feed_expr : FRESH contract_subaward projection expr. None ⇒ typed-NULL placeholder.
# Macro s(x) = nullif(nullif(trim(x),''),'-NONE-'). Typed cast: TRY_CAST(s(x) AS <T>).
# Mod-frontier: TRY_CAST(replace(s(x),'+00','') AS TIMESTAMP) (NO strptime).
# core = DUAL-SOURCED (both legs). enrich = SINGLE-SOURCE (one leg None → typed NULL on the other).
# =========================================================================================== #
COLUMN_SPEC: list[dict] = [
    # ---- (a) keys — composite PK parts + resolution keys ----
    {"canonical": "subaward_unique_key", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(unique_award_key) || '|' || s(subaward_number)",
     "feed_expr": "s(prime_award_unique_key) || '|' || s(subaward_number)"},
    {"canonical": "prime_award_unique_key", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(unique_award_key)",
     "feed_expr": "s(prime_award_unique_key)"},
    {"canonical": "subaward_number", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(subaward_number)",
     "feed_expr": "s(subaward_number)"},
    {"canonical": "subawardee_uei", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(sub_awardee_or_recipient_uei)",
     "feed_expr": "s(subawardee_uei)"},
    {"canonical": "prime_awardee_uei", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(awardee_or_recipient_uei)",
     "feed_expr": "s(prime_awardee_uei)"},
    {"canonical": "prime_awardee_parent_uei", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(ultimate_parent_uei)",
     "feed_expr": "s(prime_awardee_parent_uei)"},
    {"canonical": "prime_award_piid", "duck_type": "VARCHAR", "group": "key",
     "bulk_expr": "s(piid)",
     "feed_expr": "s(prime_award_piid)"},
    # ---- (b) core — DUAL-SOURCED reconciled facts (both legs) ----
    {"canonical": "subaward_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "subaward_amount",
     "feed_expr": "TRY_CAST(s(subaward_amount) AS DOUBLE)"},
    {"canonical": "subaward_action_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "sub_action_date",
     "feed_expr": "TRY_CAST(s(subaward_action_date) AS DATE)"},
    {"canonical": "subaward_action_date_fiscal_year", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "sub_fiscal_year",
     "feed_expr": "TRY_CAST(s(subaward_action_date_fiscal_year) AS BIGINT)"},
    {"canonical": "subaward_last_modified_date", "duck_type": "TIMESTAMP", "group": "core",
     "bulk_expr": "broker_updated_at",
     "feed_expr": "TRY_CAST(replace(s(subaward_sam_report_last_modified_date),'+00','') AS TIMESTAMP)"},
    {"canonical": "subaward_description", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(subaward_description)",
     "feed_expr": "s(subaward_description)"},
    {"canonical": "subaward_type", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(subaward_type)",
     "feed_expr": "s(subaward_type)"},
    {"canonical": "subaward_sam_report_year", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "subaward_report_year",
     "feed_expr": "TRY_CAST(s(subaward_sam_report_year) AS BIGINT)"},
    {"canonical": "subaward_sam_report_month", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "subaward_report_month",
     "feed_expr": "TRY_CAST(s(subaward_sam_report_month) AS BIGINT)"},
    {"canonical": "subawardee_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_awardee_or_recipient_legal)",
     "feed_expr": "s(subawardee_name)"},
    {"canonical": "subawardee_parent_uei", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_ultimate_parent_uei)",
     "feed_expr": "s(subawardee_parent_uei)"},
    {"canonical": "subawardee_parent_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_ultimate_parent_legal_enti)",
     "feed_expr": "s(subawardee_parent_name)"},
    {"canonical": "subawardee_dba_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_dba_name)",
     "feed_expr": "s(subawardee_dba_name)"},
    {"canonical": "subawardee_business_types", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_business_types)",
     "feed_expr": "s(subawardee_business_types)"},
    {"canonical": "subawardee_address_line_1", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_legal_entity_address_line1)",
     "feed_expr": "s(subawardee_address_line_1)"},
    {"canonical": "subawardee_city_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_legal_entity_city_name)",
     "feed_expr": "s(subawardee_city_name)"},
    {"canonical": "subawardee_state_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_legal_entity_state_code)",
     "feed_expr": "s(subawardee_state_code)"},
    {"canonical": "subawardee_zip_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_legal_entity_zip)",
     "feed_expr": "s(subawardee_zip_code)"},
    {"canonical": "subawardee_country_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_legal_entity_country_code)",
     "feed_expr": "s(subawardee_country_code)"},
    {"canonical": "subaward_recipient_cd_current", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_legal_entity_congressional_current)",
     "feed_expr": "s(subaward_recipient_cd_current)"},
    {"canonical": "subawardee_highly_compensated_officer_1_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_high_comp_officer1_full_na)",
     "feed_expr": "s(subawardee_highly_compensated_officer_1_name)"},
    {"canonical": "subawardee_highly_compensated_officer_1_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "sub_high_comp_officer1_amount",
     "feed_expr": "TRY_CAST(s(subawardee_highly_compensated_officer_1_amount) AS DOUBLE)"},
    {"canonical": "subawardee_highly_compensated_officer_2_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_high_comp_officer2_full_na)",
     "feed_expr": "s(subawardee_highly_compensated_officer_2_name)"},
    {"canonical": "subawardee_highly_compensated_officer_2_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "sub_high_comp_officer2_amount",
     "feed_expr": "TRY_CAST(s(subawardee_highly_compensated_officer_2_amount) AS DOUBLE)"},
    {"canonical": "subawardee_highly_compensated_officer_3_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_high_comp_officer3_full_na)",
     "feed_expr": "s(subawardee_highly_compensated_officer_3_name)"},
    {"canonical": "subawardee_highly_compensated_officer_3_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "sub_high_comp_officer3_amount",
     "feed_expr": "TRY_CAST(s(subawardee_highly_compensated_officer_3_amount) AS DOUBLE)"},
    {"canonical": "subawardee_highly_compensated_officer_4_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_high_comp_officer4_full_na)",
     "feed_expr": "s(subawardee_highly_compensated_officer_4_name)"},
    {"canonical": "subawardee_highly_compensated_officer_4_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "sub_high_comp_officer4_amount",
     "feed_expr": "TRY_CAST(s(subawardee_highly_compensated_officer_4_amount) AS DOUBLE)"},
    {"canonical": "subawardee_highly_compensated_officer_5_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_high_comp_officer5_full_na)",
     "feed_expr": "s(subawardee_highly_compensated_officer_5_name)"},
    {"canonical": "subawardee_highly_compensated_officer_5_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "sub_high_comp_officer5_amount",
     "feed_expr": "TRY_CAST(s(subawardee_highly_compensated_officer_5_amount) AS DOUBLE)"},
    {"canonical": "subaward_primary_place_of_performance_city_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_place_of_perform_city_name)",
     "feed_expr": "s(subaward_primary_place_of_performance_city_name)"},
    {"canonical": "subaward_primary_place_of_performance_state_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_place_of_perform_state_code)",
     "feed_expr": "s(subaward_primary_place_of_performance_state_code)"},
    {"canonical": "subaward_primary_place_of_performance_address_zip_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_place_of_performance_zip)",
     "feed_expr": "s(subaward_primary_place_of_performance_address_zip_code)"},
    {"canonical": "subaward_primary_place_of_performance_country_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_place_of_perform_country_co)",
     "feed_expr": "s(subaward_primary_place_of_performance_country_code)"},
    {"canonical": "subaward_place_of_performance_cd_current", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(sub_place_of_performance_congressional_current)",
     "feed_expr": "s(subaward_place_of_performance_cd_current)"},
    {"canonical": "prime_award_amount", "duck_type": "DOUBLE", "group": "core",
     "bulk_expr": "award_amount",
     "feed_expr": "TRY_CAST(s(prime_award_amount) AS DOUBLE)"},
    {"canonical": "prime_award_latest_action_date", "duck_type": "DATE", "group": "core",
     "bulk_expr": "action_date",
     "feed_expr": "TRY_CAST(s(prime_award_latest_action_date) AS DATE)"},
    {"canonical": "prime_award_latest_action_date_fiscal_year", "duck_type": "BIGINT", "group": "core",
     "bulk_expr": "TRY_CAST(s(fy) AS BIGINT)",
     "feed_expr": "TRY_CAST(s(prime_award_latest_action_date_fiscal_year) AS BIGINT)"},
    {"canonical": "prime_award_naics_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(naics)",
     "feed_expr": "s(prime_award_naics_code)"},
    {"canonical": "prime_award_naics_description", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(naics_description)",
     "feed_expr": "s(prime_award_naics_description)"},
    {"canonical": "prime_award_base_transaction_description", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(award_description)",
     "feed_expr": "s(prime_award_base_transaction_description)"},
    {"canonical": "prime_award_parent_piid", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(parent_award_id)",
     "feed_expr": "s(prime_award_parent_piid)"},
    {"canonical": "prime_award_awarding_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_agency_code)",
     "feed_expr": "s(prime_award_awarding_agency_code)"},
    {"canonical": "prime_award_awarding_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_agency_name)",
     "feed_expr": "s(prime_award_awarding_agency_name)"},
    {"canonical": "prime_award_awarding_sub_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_sub_tier_agency_c)",
     "feed_expr": "s(prime_award_awarding_sub_agency_code)"},
    {"canonical": "prime_award_awarding_sub_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_sub_tier_agency_n)",
     "feed_expr": "s(prime_award_awarding_sub_agency_name)"},
    {"canonical": "prime_award_awarding_office_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_office_code)",
     "feed_expr": "s(prime_award_awarding_office_code)"},
    {"canonical": "prime_award_awarding_office_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awarding_office_name)",
     "feed_expr": "s(prime_award_awarding_office_name)"},
    {"canonical": "prime_award_funding_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_agency_code)",
     "feed_expr": "s(prime_award_funding_agency_code)"},
    {"canonical": "prime_award_funding_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_agency_name)",
     "feed_expr": "s(prime_award_funding_agency_name)"},
    {"canonical": "prime_award_funding_sub_agency_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_sub_tier_agency_co)",
     "feed_expr": "s(prime_award_funding_sub_agency_code)"},
    {"canonical": "prime_award_funding_sub_agency_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_sub_tier_agency_na)",
     "feed_expr": "s(prime_award_funding_sub_agency_name)"},
    {"canonical": "prime_award_funding_office_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_office_code)",
     "feed_expr": "s(prime_award_funding_office_code)"},
    {"canonical": "prime_award_funding_office_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(funding_office_name)",
     "feed_expr": "s(prime_award_funding_office_name)"},
    {"canonical": "prime_awardee_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(awardee_or_recipient_legal)",
     "feed_expr": "s(prime_awardee_name)"},
    {"canonical": "prime_awardee_parent_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(ultimate_parent_legal_enti)",
     "feed_expr": "s(prime_awardee_parent_name)"},
    {"canonical": "prime_awardee_business_types", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(business_types)",
     "feed_expr": "s(prime_awardee_business_types)"},
    {"canonical": "prime_awardee_state_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(legal_entity_state_code)",
     "feed_expr": "s(prime_awardee_state_code)"},
    {"canonical": "prime_awardee_country_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(legal_entity_country_code)",
     "feed_expr": "s(prime_awardee_country_code)"},
    {"canonical": "prime_award_summary_recipient_cd_current", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(legal_entity_congressional_current)",
     "feed_expr": "s(prime_award_summary_recipient_cd_current)"},
    {"canonical": "prime_award_primary_place_of_performance_city_name", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_perform_city_name)",
     "feed_expr": "s(prime_award_primary_place_of_performance_city_name)"},
    {"canonical": "prime_award_primary_place_of_performance_state_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_perform_state_code)",
     "feed_expr": "s(prime_award_primary_place_of_performance_state_code)"},
    {"canonical": "prime_award_primary_place_of_performance_address_zip_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_performance_zip)",
     "feed_expr": "s(prime_award_primary_place_of_performance_address_zip_code)"},
    {"canonical": "prime_award_primary_place_of_performance_country_code", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_perform_country_co)",
     "feed_expr": "s(prime_award_primary_place_of_performance_country_code)"},
    {"canonical": "prime_award_summary_place_of_performance_cd_current", "duck_type": "VARCHAR", "group": "core",
     "bulk_expr": "s(place_of_performance_congressional_current)",
     "feed_expr": "s(prime_award_summary_place_of_performance_cd_current)"},
    # ---- (c) enrich — SINGLE-SOURCE fill (BULK-only feed_expr=None / FRESH-only bulk_expr=None) ----
    {"canonical": "subawardee_county_code", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(sub_legal_entity_county_code)",
     "feed_expr": None},
    {"canonical": "prime_award_type", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(prime_award_type)",
     "feed_expr": None},
    {"canonical": "prime_award_product_or_service_code", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(product_or_service_code)",
     "feed_expr": None},
    {"canonical": "prime_award_product_or_service_description", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(product_or_service_description)",
     "feed_expr": None},
    {"canonical": "prime_award_type_of_set_aside_code", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(type_set_aside)",
     "feed_expr": None},
    {"canonical": "prime_award_extent_competed", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(extent_competed)",
     "feed_expr": None},
    {"canonical": "prime_award_type_of_contract_pricing", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": "s(type_of_contract_pricing)",
     "feed_expr": None},
    {"canonical": "prime_award_base_action_date", "duck_type": "DATE", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "TRY_CAST(s(prime_award_base_action_date) AS DATE)"},
    {"canonical": "prime_award_base_action_date_fiscal_year", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "TRY_CAST(s(prime_award_base_action_date_fiscal_year) AS BIGINT)"},
    {"canonical": "prime_award_period_of_performance_start_date", "duck_type": "DATE", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "TRY_CAST(s(prime_award_period_of_performance_start_date) AS DATE)"},
    {"canonical": "prime_award_period_of_performance_current_end_date", "duck_type": "DATE", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "TRY_CAST(s(prime_award_period_of_performance_current_end_date) AS DATE)"},
    {"canonical": "prime_award_period_of_performance_potential_end_date", "duck_type": "DATE", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "TRY_CAST(s(prime_award_period_of_performance_potential_end_date) AS DATE)"},
    {"canonical": "prime_award_federal_accounts_funding_this_award", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "s(prime_award_federal_accounts_funding_this_award)"},
    {"canonical": "prime_award_object_classes_funding_this_award", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "s(prime_award_object_classes_funding_this_award)"},
    {"canonical": "prime_award_disaster_emergency_fund_codes", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "s(prime_award_disaster_emergency_fund_codes)"},
    {"canonical": "subaward_sam_report_id", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "s(subaward_sam_report_id)"},
    {"canonical": "broker_subaward_id", "duck_type": "BIGINT", "group": "enrich",
     "bulk_expr": "broker_subaward_id",
     "feed_expr": None},
    {"canonical": "usaspending_permalink", "duck_type": "VARCHAR", "group": "enrich",
     "bulk_expr": None,
     "feed_expr": "s(usaspending_permalink)"},
    # ---- (d) provenance ----
    {"canonical": "canonical_source", "duck_type": "VARCHAR", "group": "prov",
     "bulk_expr": None,
     "feed_expr": None},   # derived per-key = winning core row's src (fresh|bulk)
    {"canonical": "built_at", "duck_type": "TIMESTAMP", "group": "prov",
     "bulk_expr": None,
     "feed_expr": None},   # Python-injected literal per leg
]

_MACROS = "CREATE MACRO s(x) AS nullif(nullif(trim(x), ''), '-NONE-');\n"

# Index plan (locked by the column-selection pass) — presence-filtered at index() time.
BTREE_COLS = [
    "subaward_unique_key", "prime_award_unique_key", "subaward_number",
    "subawardee_uei", "prime_awardee_uei", "prime_awardee_parent_uei",
    "prime_award_piid", "subaward_amount", "subaward_action_date",
    "prime_award_naics_code", "subaward_last_modified_date",
]
BITMAP_COLS = [
    "subawardee_state_code", "subawardee_country_code", "subaward_primary_place_of_performance_state_code",
    "subaward_primary_place_of_performance_country_code", "prime_award_awarding_agency_code",
    "prime_award_awarding_sub_agency_code", "prime_award_funding_agency_code",
    "prime_award_funding_sub_agency_code", "prime_awardee_state_code", "prime_awardee_country_code",
    "prime_award_primary_place_of_performance_state_code",
    "prime_award_primary_place_of_performance_country_code", "prime_award_type",
    "prime_award_product_or_service_code", "prime_award_type_of_set_aside_code",
    "prime_award_extent_competed", "prime_award_type_of_contract_pricing",
    "prime_award_disaster_emergency_fund_codes", "canonical_source",
]

# Composite PK — the fail-closed gate and every PARTITION/JOIN key.
PK_COLS = ["prime_award_unique_key", "subaward_number"]
PK_TUPLE = "(" + ", ".join(PK_COLS) + ")"
PK_NOT_NULL = " AND ".join(f"{c} IS NOT NULL" for c in PK_COLS)
PK_JOIN = lambda a, b: " AND ".join(f"{a}.{c} = {b}.{c}" for c in PK_COLS)  # noqa: E731


# ---- COLUMN_SPEC derived helpers (all generated; nothing hand-transcribed) ---- #
def _canon_order() -> list[str]:
    return [c["canonical"] for c in COLUMN_SPEC]


def _cols(group: str) -> list[dict]:
    return [c for c in COLUMN_SPEC if c["group"] == group]


def _typed_null(c: dict) -> str:
    return f"CAST(NULL AS {c['duck_type']})"


_PARSE_SKIP = {"s", "kbulk", "TRY_CAST", "COALESCE", "CAST", "AS", "DOUBLE", "BIGINT", "DATE",
               "TIMESTAMP", "VARCHAR", "INTEGER", "replace", "nullif", "trim", "NULL",
               "upper", "lower", "substr", "concat", "concat_ws"}


def _source_cols(kind: str) -> list[str]:
    """Distinct raw source column names referenced by the bulk/feed projection exprs — the scanner
    column list. Parsed by stripping macro/cast wrappers down to bare identifiers."""
    import re
    key = "bulk_expr" if kind == "bulk" else "feed_expr"
    raw: set[str] = set()
    for c in COLUMN_SPEC:
        expr = c[key]
        if not expr:
            continue
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr):
            if tok not in _PARSE_SKIP:
                raw.add(tok)
    return sorted(raw)


# =========================================================================================== #
# Generated SQL builders (pure strings — NO R2; safe to print for inspection)
# =========================================================================================== #
def _proj_select(side: str, built_at_iso: str) -> str:
    """One per-source projection SELECT body in the canonical column order. side: 'bulk' | 'feed'.
    Enrichment cols with no source on this side project as typed-NULL placeholders. canonical_source
    is a typed-NULL placeholder here (derived per-key downstream); built_at is the injected literal."""
    lines = []
    for c in COLUMN_SPEC:
        canon = c["canonical"]
        if c["group"] == "prov":
            if canon == "canonical_source":
                lines.append(f"  {_typed_null(c)} AS canonical_source")
            else:  # built_at
                lines.append(f"  TIMESTAMP '{built_at_iso}' AS built_at")
            continue
        expr = c["bulk_expr" if side == "bulk" else "feed_expr"]
        if expr is None:
            expr = _typed_null(c)
        lines.append(f"  {expr} AS {canon}")
    return ",\n".join(lines)


def _enrich_replace_block() -> str:
    """The enrichment REPLACE block for `resolved`: overwrite every enrichment column keyed on the
    composite, INDEPENDENT of which source won the core. Each enrich col is SINGLE-SOURCE:
      • BULK-only  (feed_expr None) → b.<col> from bulk_latest
      • FRESH-only (bulk_expr None) → f.<col> from fresh_latest
    No COALESCE (no dual-source enrich exists). b/f are the LEFT-JOINed PK-unique collapses."""
    parts = []
    for c in _cols("enrich"):
        col = c["canonical"]
        leg = "b" if c["bulk_expr"] is not None else "f"
        parts.append(f"    {leg}.{col} AS {col}")
    return ",\n".join(parts)


def _stage1_sql(built_at_iso: str) -> str:
    """STAGE 1 — macros, the two per-source projections, and the two latest-per-composite collapses.
    Small-data (2.6M contract BULK + 321K FRESH): projections are materialized (re-scannable) so
    rows_in/null-key metrics are exact — no single-pass inlining is needed (that was the 107M prime
    spine's spill-avoidance). Each collapse orders by the unified mod-frontier then that source's own
    surrogate (BULK broker_subaward_id / FRESH subaward_sam_report_id)."""
    bulk_proj = _proj_select("bulk", built_at_iso)
    fresh_proj = _proj_select("feed", built_at_iso)
    return f"""{_MACROS}
-- ===== per-source projections (canonical vocabulary; identical shape by construction) ===== --
CREATE TEMP TABLE bulk_proj AS
SELECT
{bulk_proj}
FROM bulk_r;

CREATE TEMP TABLE fresh_proj AS
SELECT
{fresh_proj}
FROM fresh_r;

-- ===== BULK collapse → latest-per-composite (broker_subaward_id row surrogate tie-break) ===== --
CREATE TEMP TABLE bulk_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY {', '.join(PK_COLS)}
            ORDER BY subaward_last_modified_date DESC NULLS LAST,
                     broker_subaward_id DESC NULLS LAST) AS rn
  FROM bulk_proj
  WHERE {PK_NOT_NULL}
) WHERE rn = 1;

-- ===== FRESH collapse → latest-per-composite (subaward_sam_report_id surrogate tie-break) ===== --
-- FRESH re-pulls overlapping windows on its daily append (FSRS publish lag) → duplicate
-- subaward_sam_report_id rows; this collapse deterministically keeps the latest report per composite.
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY {', '.join(PK_COLS)}
            ORDER BY subaward_last_modified_date DESC NULLS LAST,
                     subaward_sam_report_id DESC NULLS LAST) AS rn
  FROM fresh_proj
  WHERE {PK_NOT_NULL}
) WHERE rn = 1;
"""


def _stage2_sql() -> str:
    """STAGE 2 — the TWO-SOURCE merge, ONE physical artifact.
      core_union  (UNION ALL BY NAME of the two collapsed cores, tagged src + source_rank FRESH<BULK)
        → core_winner (SINGLE 2-way window: argmax(subaward_last_modified_date) per composite,
                       source_rank tiebreak → tie=FRESH; associativity-equivalent to fresh⊕bulk)
        → resolved   (LEFT JOIN bulk_latest [b] + fresh_latest [f] → single-source enrich REPLACE +
                     w.src AS canonical_source)
        → canonical_out (locked canonical projection). NO tombstone leg (subawards are superseded,
                     never deleted). PK-uniqueness is structural (row_number()=1 per composite)."""
    enrich_block = _enrich_replace_block()
    canon_cols = ", ".join(_canon_order())
    return f"""-- ===== two collapsed cores → vertical union, tagged src + source_rank (FRESH<BULK) ===== --
CREATE TEMP TABLE core_union AS
SELECT CAST('fresh' AS VARCHAR) AS src, CAST(1 AS INTEGER) AS source_rank, f.* FROM fresh_latest f
UNION ALL BY NAME
SELECT CAST('bulk'  AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, b.* FROM bulk_latest b;

-- ===== SINGLE 2-way per-composite core resolution: argmax(subaward_last_modified_date), tie→FRESH ==== --
-- After the two collapses there is at most one row per source per composite → source_rank alone
-- disambiguates every cross-source tie; subaward_unique_key DESC is defense-in-depth. Exactly one
-- survivor per composite (row_number()=1) → PK-uniqueness structural.
CREATE TEMP TABLE core_winner AS
SELECT * EXCLUDE (rn, source_rank) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY {', '.join(PK_COLS)}
            ORDER BY subaward_last_modified_date DESC NULLS LAST,
                     source_rank ASC,
                     subaward_unique_key DESC) AS rn
  FROM core_union
) WHERE rn = 1;

-- ===== enrichment fill: BULK-only from bulk_latest (b), FRESH-only from fresh_latest (f) ===== --
-- Both LEFT JOINs are to PK-unique per-composite collapses → no fan-out. canonical_source is derived
-- HERE, once, as the winning core row's src. EXCLUDE the typed-NULL canonical_source placeholder
-- carried up from the projections (else it collides with w.src AS canonical_source and DuckDB
-- silently renames the derived column).
CREATE TEMP TABLE resolved AS
SELECT
  w.* EXCLUDE (src, canonical_source) REPLACE (
{enrich_block}
  ),
  w.src AS canonical_source
FROM core_winner w
LEFT JOIN bulk_latest  b ON {PK_JOIN('w', 'b')}
LEFT JOIN fresh_latest f ON {PK_JOIN('w', 'f')};

-- ===== locked canonical projection ===== --
CREATE TEMP TABLE canonical_out AS
SELECT {canon_cols} FROM resolved;
"""


def _build_merge_sql(built_at_iso: str) -> str:
    """FULL merge SQL (stage 1 + stage 2) concatenated — for print_merge_sql inspection ONLY. build()
    runs the two stages separately so the schema-identity gate runs between them."""
    return (_stage1_sql(built_at_iso)
            + "\n-- [build() runs the schema-identity gate HERE, on the two collapses]\n"
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


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={DUCK_THREADS}")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    con.execute("SET preserve_insertion_order=false")
    return con


def _assert_collapse_schema_identity(con) -> None:
    """The two collapses (SELECT * EXCLUDE(rn) over projection-shaped inners) MUST be byte-identical
    (name, type) before the union. Raise on mismatch — a hard build failure rather than a silent
    transposition (paired with UNION ALL BY NAME downstream)."""
    sigs = {}
    for name in ("bulk_latest", "fresh_latest"):
        rows = con.execute(f"DESCRIBE {name}").fetchall()
        sigs[name] = [(r[0], r[1]) for r in rows]
    if sigs["bulk_latest"] != sigs["fresh_latest"]:
        diff = [(b, f) for b, f in zip(sigs["bulk_latest"], sigs["fresh_latest"]) if b != f]
        raise RuntimeError(f"collapse schema mismatch bulk_latest vs fresh_latest; "
                           f"first divergences: {diff[:5]}")


def _record_run(*, rows_in_bulk_contract, rows_in_fresh, rows_out, dedup_collapsed, fresh_only_tail,
                bulk_only_body, fresh_corrections_applied, null_key_dropped, max_subaward_action_date,
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
                "INSERT INTO ops.usaspending_subaward_canonical_runs (feed, rows_in_bulk_contract, "
                "rows_in_fresh, rows_out, dedup_collapsed, fresh_only_tail, bulk_only_body, "
                "fresh_corrections_applied, null_key_dropped, max_subaward_action_date, columns, "
                "write_mode, indices_built, status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (FEED, rows_in_bulk_contract, rows_in_fresh, rows_out, dedup_collapsed, fresh_only_tail,
                 bulk_only_body, fresh_corrections_applied, null_key_dropped, max_subaward_action_date,
                 columns, write_mode, ",".join(indices_built) if indices_built else None, status,
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


def build(since: str | None = None, target_uri: str = CANONICAL_URI) -> dict:
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    built_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    built_at_iso = built_at.strftime("%Y-%m-%d %H:%M:%S.%f")

    status, error = "error", None
    rows_in_bulk = rows_in_fresh = rows_out = dedup_collapsed = 0
    fresh_only_tail = bulk_only_body = fresh_corrections = null_key_dropped = 0
    max_action = None
    metrics: dict = {}
    built_idx: list[str] = []
    con = None
    try:
        # --since pushed into BOTH data scanners. BULK sub_action_date is date32; FRESH
        # subaward_action_date is lexical ISO-10 VARCHAR. NEVER changes the contract scope.
        bulk_filter = CONTRACT_FILTER
        feed_filter = None
        if since:
            bulk_filter += f" AND sub_action_date >= DATE '{since}'"
            feed_filter = f"subaward_action_date >= '{since}'"

        bulk_ds = lance.dataset(BULK_URI, storage_options=so)
        fresh_ds = lance.dataset(FRESH_URI, storage_options=so)
        bulk_present = set(bulk_ds.schema.names)
        fresh_present = set(fresh_ds.schema.names)
        bulk_scan = [c for c in _source_cols("bulk") if c in bulk_present]
        fresh_scan = [c for c in _source_cols("feed") if c in fresh_present]

        con = _duck()
        log(f"registering sources (since={since}, contract-only) → target {target_uri}")
        # Small data (re-scannable) → .to_table(); exact rows_in + null-key metrics.
        con.register("bulk_r", bulk_ds.scanner(columns=bulk_scan, filter=bulk_filter).to_table())
        con.register("fresh_r", fresh_ds.scanner(columns=fresh_scan, filter=feed_filter).to_table())

        con.execute(_stage1_sql(built_at_iso))
        _assert_collapse_schema_identity(con)
        con.execute(_stage2_sql())

        rows_in_bulk = con.execute("SELECT count(*) FROM bulk_proj").fetchone()[0]
        rows_in_fresh = con.execute("SELECT count(*) FROM fresh_proj").fetchone()[0]
        null_key_dropped = con.execute(
            f"SELECT (SELECT count(*) FROM bulk_proj WHERE NOT ({PK_NOT_NULL})) + "
            f"(SELECT count(*) FROM fresh_proj WHERE NOT ({PK_NOT_NULL}))").fetchone()[0]
        merged = con.execute("SELECT count(*) FROM core_winner").fetchone()[0]

        rows_out, pk_distinct, suk_distinct = con.execute(
            f"SELECT count(*), count(DISTINCT {PK_TUPLE}), count(DISTINCT subaward_unique_key) "
            "FROM canonical_out").fetchone()
        # FAIL-CLOSED gate — raise BEFORE publish. (1) PK-unique: one row per composite.
        if rows_out != pk_distinct:
            raise RuntimeError(
                f"PK gate FAILED: count(*)={rows_out:,} != distinct{PK_TUPLE}={pk_distinct:,} "
                f"({rows_out - pk_distinct:,} dup composites). Aborting publish.")
        # (2) synthesized-key collision: subaward_unique_key must be 1:1 with the composite (else the
        # '|' separator collided two distinct composites → the BTREE point-lookup key would be wrong).
        if suk_distinct != pk_distinct:
            raise RuntimeError(
                f"subaward_unique_key collision: distinct suk={suk_distinct:,} != distinct composite="
                f"{pk_distinct:,}. The '|' separator collided distinct composites. Aborting publish.")

        dedup_collapsed = int(rows_in_bulk + rows_in_fresh - null_key_dropped - merged)
        fresh_only_tail = con.execute(
            f"SELECT count(*) FROM fresh_latest f ANTI JOIN bulk_latest b ON {PK_JOIN('f', 'b')}"
        ).fetchone()[0]
        bulk_only_body = con.execute(
            f"SELECT count(*) FROM bulk_latest b ANTI JOIN fresh_latest f ON {PK_JOIN('b', 'f')}"
        ).fetchone()[0]
        # fresh_corrections_applied: composites FRESH won that BULK also holds (landed FSRS corrections).
        fresh_corrections = con.execute(
            f"SELECT count(*) FROM canonical_out co SEMI JOIN bulk_latest b ON {PK_JOIN('co', 'b')} "
            "WHERE co.canonical_source = 'fresh'").fetchone()[0]
        # max action date clamped to a sane frontier (FSRS carries 2106 sentinels; carried RAW on-spine).
        max_action = con.execute(
            "SELECT max(subaward_action_date) FROM canonical_out "
            "WHERE subaward_action_date <= CURRENT_DATE").fetchone()[0]

        log(f"core_winner={merged:,} rows_out={rows_out:,} fresh_only_tail={fresh_only_tail:,} "
            f"bulk_only_body={bulk_only_body:,} fresh_corrections={fresh_corrections:,} "
            f"null_key_dropped={null_key_dropped:,} max_action_date={max_action}")

        # ── DIRECT-R2 write (non-giant: ~1.3M rows, proven pattern) — streaming reader, low RAM ──
        reader = con.sql("SELECT * FROM canonical_out").to_arrow_reader(batch_size=200_000)
        log(f"writing Lance DIRECT-R2 → {target_uri}")
        lance.write_dataset(reader, target_uri, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE,
                            storage_options=so)
        con.close()
        con = None
        committed = lance.dataset(target_uri, storage_options=so).count_rows()
        status = "success"
        log(f"DONE → {target_uri} committed={committed:,}")
        metrics = {"target_uri": target_uri, "since": since,
                   "rows_in_bulk_contract": int(rows_in_bulk), "rows_in_fresh": int(rows_in_fresh),
                   "rows_out": int(rows_out), "dedup_collapsed": int(dedup_collapsed),
                   "fresh_only_tail": int(fresh_only_tail), "bulk_only_body": int(bulk_only_body),
                   "fresh_corrections_applied": int(fresh_corrections),
                   "null_key_dropped": int(null_key_dropped),
                   "max_subaward_action_date": max_action, "pk_unique": True,
                   "columns": len(COLUMN_SPEC), "committed_rows": int(committed),
                   "write_mode": "overwrite", "status": status}
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        log(f"FAILED: {error}")
        raise
    finally:
        if con is not None:
            con.close()
        _record_run(rows_in_bulk_contract=int(rows_in_bulk), rows_in_fresh=int(rows_in_fresh),
                    rows_out=int(rows_out), dedup_collapsed=int(dedup_collapsed),
                    fresh_only_tail=int(fresh_only_tail), bulk_only_body=int(bulk_only_body),
                    fresh_corrections_applied=int(fresh_corrections),
                    null_key_dropped=int(null_key_dropped), max_subaward_action_date=max_action,
                    columns=len(COLUMN_SPEC), write_mode="overwrite", indices_built=built_idx,
                    status=status, error=error, started=started,
                    completed=dt.datetime.now(dt.timezone.utc))
    return metrics


def index(target_uri: str = CANONICAL_URI) -> dict:
    """Build the BTREE/BITMAP scalar indices DIRECT-R2, in place. At ~1.3M rows this is far under the
    ~100M "giant" threshold that forces the prime spine's Volume-staged local build (R2's uniform-part
    rule only trips on giant scalar-index page_data). LANCE_BYPASS_SPILLING keeps the small sort in-RAM.
    Kept a separate subcommand from build() for blast-radius isolation. Idempotent (replace=True)."""
    import lance
    ds = lance.dataset(target_uri, storage_options=_r2_so())
    present = set(ds.schema.names)
    built: list[str] = []
    log(f"indexing DIRECT-R2 ({ds.count_rows():,} rows) → {target_uri}")
    for col, kind in ([(c, "BTREE") for c in BTREE_COLS if c in present]
                      + [(c, "BITMAP") for c in BITMAP_COLS if c in present]):
        try:
            ds.create_scalar_index(col, index_type=kind, replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type=kind)
        built.append(col)
        log(f"  {kind} ✓ {col}")
    return {"target_uri": target_uri, "indices_built": built}


def verify(target_uri: str = CANONICAL_URI) -> dict:
    """Read-back proof (independent scanner → DuckDB). GATES (verdict=fail): PK-unique on the composite;
    subaward_unique_key 1:1 with the composite; built_at single literal; canonical_source ⊆ {fresh,bulk};
    fresh_corrections_applied > 0 (FRESH must win at least one shared composite)."""
    import lance
    so = _r2_so()
    ds = lance.dataset(target_uri, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    con.register("c_src", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE c AS SELECT * FROM c_src")

    total, pk_distinct, suk_distinct = con.execute(
        f"SELECT count(*), count(DISTINCT {PK_TUPLE}), count(DISTINCT subaward_unique_key) FROM c"
    ).fetchone()
    mx_action = con.execute(
        "SELECT max(subaward_action_date) FROM c WHERE subaward_action_date <= CURRENT_DATE").fetchone()[0]
    src_dist = dict(con.execute(
        "SELECT canonical_source, count(*) FROM c GROUP BY 1 ORDER BY 2 DESC").fetchall())
    built_at_distinct = con.execute("SELECT count(DISTINCT built_at) FROM c").fetchone()[0]
    null_key = con.execute(f"SELECT count(*) FROM c WHERE NOT ({PK_NOT_NULL})").fetchone()[0]
    bad_source = con.execute(
        "SELECT count(*) FROM c WHERE canonical_source IS NULL "
        "OR canonical_source NOT IN ('fresh','bulk')").fetchone()[0]
    # fresh_corrections proxy on the read model: count of fresh-won rows (cross-source membership can't
    # be re-derived from the single canonical, but fresh must have won SOME core → > 0).
    fresh_won = con.execute("SELECT count(*) FROM c WHERE canonical_source = 'fresh'").fetchone()[0]
    con.close()

    failures: list[str] = []
    if total != pk_distinct:
        failures.append(f"PK-unique: count(*)={total:,} != distinct{PK_TUPLE}={pk_distinct:,}")
    if suk_distinct != pk_distinct:
        failures.append(f"subaward_unique_key collision: {suk_distinct:,} != composite {pk_distinct:,}")
    if built_at_distinct != 1:
        failures.append(f"built_at not a single literal: distinct={built_at_distinct}")
    if bad_source:
        failures.append(f"canonical_source domain: {bad_source:,} rows NULL or ∉ {{fresh,bulk}}")
    if fresh_won <= 0:
        failures.append("canonical_source='fresh' count == 0 — FRESH never won a core")

    return {"uri": target_uri, "rows_out": int(total),
            "pk_unique": bool(total == pk_distinct), "pk_dupes": int(total - pk_distinct),
            "subaward_unique_key_distinct": int(suk_distinct), "null_key_rows": int(null_key),
            "max_subaward_action_date": str(mx_action) if mx_action is not None else None,
            "built_at_distinct": int(built_at_distinct),
            "canonical_source_distribution": {k: int(v) for k, v in src_dist.items()},
            "fresh_won_rows": int(fresh_won), "columns": len(ds.schema.names), "indices": idx,
            "failures": failures, "pass": not failures}


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST the flat terminal metadata to the Trigger.dev waitpoint callback URL (ARCHITECTURE §5 —
    the worker owns terminal state). No-op for manual runs (url None)."""
    if not url:
        log("no trigger_callback_url (manual run); skipping callback")
        return
    import time

    import requests
    for i in range(attempts):
        try:
            r = requests.post(url, json=payload, timeout=30)
            if r.status_code < 300:
                log(f"callback delivered: {payload.get('status')}")
                return
        except Exception as exc:  # noqa: BLE001
            log(f"callback attempt {i + 1} failed: {exc}")
        time.sleep(2 * (i + 1))
    log(f"WARN: callback delivery failed after {attempts} attempts → {url}")


def refresh(trigger_callback_url: str | None = None, since: str | None = None,
            target_uri: str = CANONICAL_URI) -> dict:
    """Cadence entrypoint — the full build → index → verify chain as ONE terminal unit, then POST the
    Trigger callback. build()/index() each own their ops-ledger + blast-radius semantics; verify()
    gates the published table. Raises (→ callback status='error') on a failed verify so a stale/broken
    publish never reports success."""
    status, payload = "error", {"feed": FEED, "dataset_uri": target_uri}
    try:
        b = build(since=since, target_uri=target_uri)
        idx = index(target_uri=target_uri)
        v = verify(target_uri=target_uri)
        status = "success" if v.get("pass") else "error"
        payload.update({"status": status, "rows_out": b.get("rows_out"),
                        "fresh_corrections_applied": b.get("fresh_corrections_applied"),
                        "indices_built": len(idx.get("indices_built", [])),
                        "verify_pass": bool(v.get("pass")), "verify_failures": v.get("failures")})
        if status != "success":
            raise RuntimeError(f"verify failed: {v.get('failures')}")
        return {**b, "index": idx, "verify": v}
    except BaseException as exc:  # noqa: BLE001
        payload.setdefault("status", "error")
        payload["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        _post_callback(trigger_callback_url, payload)


# =========================================================================================== #
# Modal entrypoint — the cadence/production substrate (Trigger.dev-scheduled). The dataset is small
# so no ephemeral_disk giant is needed; the local CLI above is the dev + on-box full-build path.
#   modal run pipelines/usaspending/usaspending_subaward_canonical.py                 # build
#   modal run pipelines/usaspending/usaspending_subaward_canonical.py --cmd index
#   modal run pipelines/usaspending/usaspending_subaward_canonical.py --cmd verify
# =========================================================================================== #
if modal is not None:
    _image = (
        modal.Image.debian_slim(python_version="3.12")
        .pip_install("duckdb>=1.5,<2", "pylance>=7", "lancedb>=0.15", "pyarrow>=17",
                     "psycopg[binary]>=3.2", "requests>=2.32")
        .env({
            "SUBAWARD_CANONICAL_DUCKDB_MEM": "48GB",
            "SUBAWARD_CANONICAL_DUCKDB_THREADS": "8",
            "SUBAWARD_CANONICAL_DUCKDB_TEMP_DIR": "/tmp/subaward_canonical_duckdb",
        })
    )
    modal_app = modal.App("usaspending-subaward-canonical", image=_image)
    _SECRETS = [modal.Secret.from_name("r2-credentials"), modal.Secret.from_name("hqx-postgres")]

    @modal_app.function(secrets=_SECRETS, timeout=60 * 60 * 3, memory=65_536, cpu=8.0, retries=0)
    def build_fn(since: str | None = None, target_uri: str = CANONICAL_URI) -> dict:
        return build(since=since, target_uri=target_uri)

    @modal_app.function(secrets=_SECRETS, timeout=60 * 60 * 2, memory=65_536, cpu=8.0, retries=0)
    def index_fn(target_uri: str = CANONICAL_URI) -> dict:
        return index(target_uri=target_uri)

    @modal_app.function(secrets=_SECRETS, timeout=60 * 45, memory=32_768, cpu=4.0, retries=0)
    def verify_fn(target_uri: str = CANONICAL_URI) -> dict:
        return verify(target_uri=target_uri)

    # Cadence target — Trigger.dev v4 dispatches THIS (build→index→verify + waitpoint callback).
    @modal_app.function(secrets=_SECRETS, timeout=60 * 60 * 3, memory=65_536, cpu=8.0, retries=0)
    def refresh_fn(trigger_callback_url: str | None = None, since: str | None = None,
                   target_uri: str = CANONICAL_URI) -> dict:
        return refresh(trigger_callback_url=trigger_callback_url, since=since, target_uri=target_uri)

    @modal_app.local_entrypoint()
    def modal_main(cmd: str = "build", since: str = "", target_uri: str = CANONICAL_URI):
        s = since or None
        if cmd == "build":
            print(json.dumps(build_fn.remote(since=s, target_uri=target_uri), indent=2, default=str))
        elif cmd == "build_spawn":
            call = build_fn.spawn(since=s, target_uri=target_uri)
            print(json.dumps({"spawned": "build_fn", "call_id": call.object_id,
                              "target_uri": target_uri, "since": s}, default=str))
        elif cmd == "index":
            print(json.dumps(index_fn.remote(target_uri=target_uri), indent=2, default=str))
        elif cmd == "verify":
            print(json.dumps(verify_fn.remote(target_uri=target_uri), indent=2, default=str))
        elif cmd == "refresh":
            print(json.dumps(refresh_fn.remote(since=s, target_uri=target_uri), indent=2, default=str))
        else:
            raise SystemExit(f"unknown --cmd: {cmd} (build|build_spawn|index|verify|refresh)")


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
        print(json.dumps(build(since=_arg_val("--since", argv, None), target_uri=target_uri),
                         indent=2, default=str))
    elif cmd == "index":
        print(json.dumps(index(target_uri=target_uri), indent=2, default=str))
    elif cmd == "verify":
        print(json.dumps(verify(target_uri=target_uri), indent=2, default=str))
    elif cmd == "print_merge_sql":
        print(_build_merge_sql(built_at_iso="2026-07-02 00:00:00.000000"))
    else:
        print(f"unknown command: {cmd} (init_ops|build|index|verify|print_merge_sql)")
        sys.exit(2)


if __name__ == "__main__":
    main()
