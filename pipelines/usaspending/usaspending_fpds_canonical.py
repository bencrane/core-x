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

MERGE (design_spec.md §3, defect-resolved):
  • s()/kbulk() sentinel macros applied IDENTICALLY on every source — whole-string ''/'-NONE-' → NULL.
  • fresh_latest: FRESH deduped to latest-per-key (deterministic tiebreaker).
  • bulk_latest: ONE per-key collapse over FULL BULK (enrichment-maximizing deterministic dedup),
    serving THREE roles — (A) the per-key precedence probe, (B) the uniform enrichment source for
    EVERY leg, (C) the BULK-only survivor body. 109M scanned ONCE.
  • Precedence (BLOCKER-1): per shared FRESH∩BULK key the volatile-core winner is MAX(last_modified_date),
    tie-break FRESH — BULK wins the core ONLY when STRICTLY newer; NOT "FRESH always wins".
  • Enrichment (BLOCKER-2): always sourced from bulk_latest per key, uniformly across all legs.
  • bulk_only + arch_survivors anti-joins (archive anti-joins bulk_keys_full, the FULL BULK universe).
  • UNION ALL BY NAME of the three disjoint key universes (no positional transposition).
  • Tombstone: ANTI JOIN on the delta-'D' keys. The delta scanner is filtered ONLY by
    correction_delete_ind='D' and NEVER receives --since (all 656 'D' rows have action_date=NULL).
  • Fail-closed PK-uniqueness gate raises BEFORE publish on any dup.

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
def _proj_select(side: str, source_literal: str, built_at_iso: str) -> str:
    """Generate ONE per-source projection SELECT body in the canonical column order.
    side: 'bulk' | 'feed'. For 'feed', enrichment columns project as typed NULL placeholders.
    Provenance: canonical_source literal + built_at injected literal."""
    lines = []
    for c in COLUMN_SPEC:
        canon = c["canonical"]
        if c["group"] == "prov":
            if canon == "canonical_source":
                lines.append(f"  '{source_literal}' AS canonical_source")
            else:  # built_at
                lines.append(f"  TIMESTAMP '{built_at_iso}' AS built_at")
            continue
        if side == "bulk":
            expr = c["bulk_expr"] if c["bulk_expr"] is not None else _typed_null(c)
        else:  # feed
            expr = c["feed_expr"] if c["feed_expr"] is not None else _typed_null(c)
        lines.append(f"  {expr} AS {canon}")
    return ",\n".join(lines)


def _b_wins_replace_block() -> str:
    """The §3.5 REPLACE block for the FRESH leg: (i) precedence-resolved volatile-core via the
    SINGLE b_wins predicate applied to EVERY core column + last_modified_date; (ii) uniform
    enrichment always from bulk_latest (b). PROGRAM-GENERATED from COLUMN_SPEC — no hand-transcription."""
    b_wins = "(b.k IS NOT NULL AND b.last_modified_date > f.last_modified_date)"
    parts = []
    # (i) volatile-core (group 'core') — every column, identical predicate.
    for c in _cols("core"):
        col = c["canonical"]
        parts.append(f"    CASE WHEN {b_wins} THEN b.{col} ELSE f.{col} END AS {col}")
    # (ii) enrichment — always b (NULL when no BULK match: the genuine FRESH-only tail).
    for c in _cols("enrich"):
        col = c["canonical"]
        parts.append(f"    b.{col} AS {col}")
    return ",\n".join(parts)


def _arch_enrich_replace_block() -> str:
    """archive_final REPLACE: enrichment only (b.* is NULL by construction — arch keys ∉ BULK).
    Generated identically to the (ii) enrichment half so the REPLACE targets match the other legs."""
    parts = [f"    b.{c['canonical']} AS {c['canonical']}" for c in _cols("enrich")]
    return ",\n".join(parts)


def _projections_sql(built_at_iso: str) -> str:
    """The macros + the THREE per-source projection CREATEs (design §3.1). Executed as ONE
    multi-statement script (DuckDB con.execute handles ;-separated statements natively), THEN the
    schema-identity gate runs against bulk_proj/fresh_proj/archive_proj before the merge tail."""
    bulk_proj = _proj_select("bulk", "bulk", built_at_iso)
    fresh_proj = _proj_select("feed", "fresh", built_at_iso)
    arch_proj = _proj_select("feed", "archive_full", built_at_iso)
    return f"""{_MACROS}
-- ===== §3.1 per-source projections (identical canonical column NAME+ORDER+TYPE) ===== --
CREATE TEMP TABLE bulk_proj AS
SELECT
{bulk_proj}
FROM bulk_r;

CREATE TEMP TABLE fresh_proj AS
SELECT
{fresh_proj}
FROM fresh_r;

CREATE TEMP TABLE archive_proj AS
SELECT
{arch_proj}
FROM archive_r;
"""


def _merge_tail_sql() -> str:
    """The merge tail (design §3.2–§3.6): dedup → bulk_latest collapse → survivor universes →
    precedence-resolved core + uniform enrichment → UNION ALL BY NAME → tombstone → canonical_out.
    Pure string; references the *_proj TEMP TABLEs created by _projections_sql + the registered
    archive_delta_D relation. Executed as ONE multi-statement script."""
    b_wins_block = _b_wins_replace_block()
    arch_enrich_block = _arch_enrich_replace_block()
    canon_cols = ", ".join(_canon_order())
    return f"""-- ===== §3.2 FRESH dedup → latest-per-key (deterministic tiebreaker) ===== --
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY contract_transaction_unique_key
            ORDER BY last_modified_date DESC NULLS LAST,
                     (federal_action_obligation IS NULL) ASC,
                     modification_number DESC NULLS LAST,
                     contract_award_unique_key DESC NULLS LAST) AS rn
  FROM fresh_proj
  WHERE contract_transaction_unique_key IS NOT NULL
) WHERE rn = 1;
CREATE TEMP TABLE fresh_keys AS
  SELECT DISTINCT contract_transaction_unique_key AS k FROM fresh_latest;

-- ===== §3.3 single bulk_latest collapse (one row per BULK txn_key; roles A/B/C) ===== --
-- enrichment-maximizing deterministic dedup: latest mtime, then prefer populated enrichment,
-- then stable transaction_id surrogate.
CREATE TEMP TABLE bulk_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *,
         row_number() OVER (
           PARTITION BY contract_transaction_unique_key
           ORDER BY last_modified_date DESC NULLS LAST,
                    (recipient_hash IS NULL) ASC,
                    transaction_id DESC NULLS LAST
         ) AS rn
  FROM bulk_proj
  WHERE contract_transaction_unique_key IS NOT NULL
) WHERE rn = 1;
CREATE TEMP TABLE bulk_keys_full AS
  SELECT contract_transaction_unique_key AS k FROM bulk_latest;

-- ===== §3.3b survivor key universes (disjoint by construction) ===== --
CREATE TEMP TABLE bulk_only AS
SELECT b.* FROM bulk_latest b
ANTI JOIN fresh_keys f ON b.contract_transaction_unique_key = f.k;

CREATE TEMP TABLE arch_survivors AS
SELECT a.* FROM archive_proj a
ANTI JOIN fresh_keys     f ON a.contract_transaction_unique_key = f.k
ANTI JOIN bulk_keys_full bk ON a.contract_transaction_unique_key = bk.k
WHERE a.contract_transaction_unique_key IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY a.contract_transaction_unique_key
                           ORDER BY a.last_modified_date DESC NULLS LAST,
                                    (a.federal_action_obligation IS NULL) ASC,
                                    a.contract_award_unique_key DESC NULLS LAST) = 1;

-- ===== §3.5 precedence-resolved volatile-core + uniform enrichment from bulk_latest ===== --
-- b_wins := (b.k IS NOT NULL AND b.last_modified_date > f.last_modified_date)  -- STRICTLY newer; tie→FRESH
CREATE TEMP TABLE bl_probe AS
  SELECT *, contract_transaction_unique_key AS k FROM bulk_latest;

CREATE TEMP TABLE fresh_final AS
SELECT
  f.* REPLACE (
{b_wins_block}
  )
FROM fresh_latest f
LEFT JOIN bl_probe b ON f.contract_transaction_unique_key = b.k;

CREATE TEMP TABLE arch_final AS
SELECT
  a.* REPLACE (
{arch_enrich_block}
  )
FROM arch_survivors a
LEFT JOIN bl_probe b ON a.contract_transaction_unique_key = b.k;

CREATE TEMP TABLE bulk_final AS SELECT * FROM bulk_only;

-- ===== §3.6 union three disjoint survivor sets (BY NAME) → tombstone anti-join ===== --
CREATE TEMP TABLE merged AS
SELECT * FROM fresh_final
UNION ALL BY NAME SELECT * FROM bulk_final
UNION ALL BY NAME SELECT * FROM arch_final;

-- delta scanner is filtered ONLY by correction_delete_ind='D' (caller side); NEVER --since.
CREATE TEMP TABLE delete_keys AS
SELECT DISTINCT s(contract_transaction_unique_key) AS k,
       max(TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP)) AS delta_lmt
FROM archive_delta_D
WHERE s(contract_transaction_unique_key) IS NOT NULL
GROUP BY 1;

CREATE TEMP TABLE canonical AS
SELECT m.* FROM merged m
ANTI JOIN delete_keys d ON m.contract_transaction_unique_key = d.k;

-- final projection in the locked canonical order (defensive — UNION BY NAME already aligned)
CREATE TEMP TABLE canonical_out AS
SELECT {canon_cols} FROM canonical;
"""


def _build_merge_sql(*, built_at_iso: str, since: str | None) -> str:
    """The FULL merge SQL (projections + tail) concatenated — for inspection / print_merge_sql ONLY.
    build() executes _projections_sql() and _merge_tail_sql() separately so the schema-identity gate
    can run between them. No R2 access here; safe to print.

    --since note: the predicate is pushed into the THREE DATA scanners (caller/build side), NEVER the
    delta scanner. Carried here only as a comment marker for traceability."""
    since_note = (f"-- --since={since} pushed into the THREE data scanners only "
                  f"(delta NEVER filtered)\n" if since else "")
    return since_note + _projections_sql(built_at_iso) + "\n" + _merge_tail_sql()


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
                fresh_only_tail, deletes_tombstoned, max_action_date, columns, write_mode,
                indices_built, status, error, started, completed) -> None:
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
                "max_action_date, columns, write_mode, indices_built, status, error_message, "
                "started_at, completed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (FEED, rows_in_bulk, rows_in_fresh, rows_in_archive_full, rows_out, dedup_collapsed,
                 fresh_only_tail, deletes_tombstoned, max_action_date, columns, write_mode,
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


def _assert_projection_schema_identity(con) -> None:
    """§4 enforcement — PROGRAMMATIC, not aspirational. The three *_proj relations MUST emit
    byte-identical (name, type) sequences before any union. Raise on mismatch (hard build failure
    rather than a silent transposition)."""
    sigs = {}
    for name in ("bulk_proj", "fresh_proj", "archive_proj"):
        rows = con.execute(f"DESCRIBE {name}").fetchall()  # (column_name, column_type, ...)
        sigs[name] = [(r[0], r[1]) for r in rows]
    base = sigs["bulk_proj"]
    for name, sig in sigs.items():
        if sig != base:
            diff = [(b, s) for b, s in zip(base, sig) if b != s]
            raise RuntimeError(
                f"projection schema mismatch: {name} != bulk_proj. "
                f"first divergences (bulk_proj vs {name}): {diff[:5]}")


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
        delta_scan_cols = [c for c in ("contract_transaction_unique_key", "last_modified_date",
                                       "correction_delete_ind") if c in delta_present]

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

        # Phase 1: build the three projections (one multi-statement script), then ENFORCE schema
        # identity before any union (§4 — programmatic gate). Phase 2: the merge tail.
        con.execute(_projections_sql(built_at_iso))
        _assert_projection_schema_identity(con)
        con.execute(_merge_tail_sql())

        rows_in_bulk = con.execute("SELECT count(*) FROM bulk_proj").fetchone()[0]
        rows_in_fresh = con.execute("SELECT count(*) FROM fresh_proj").fetchone()[0]
        rows_in_archive_full = con.execute("SELECT count(*) FROM archive_proj").fetchone()[0]

        rows_out = con.execute("SELECT count(*) FROM canonical_out").fetchone()[0]
        pk_total, pk_distinct = con.execute(
            "SELECT count(*), count(DISTINCT contract_transaction_unique_key) FROM canonical_out"
        ).fetchone()
        # FAIL-CLOSED PK gate — raise BEFORE publish on any dup.
        if pk_total != pk_distinct:
            raise RuntimeError(
                f"PK-uniqueness gate FAILED: count(*)={pk_total:,} != "
                f"count(DISTINCT contract_transaction_unique_key)={pk_distinct:,} "
                f"({pk_total - pk_distinct:,} dup keys). Aborting publish.")

        fresh_only_tail = con.execute(
            "SELECT count(*) FROM fresh_latest f ANTI JOIN bulk_keys_full b "
            "ON f.contract_transaction_unique_key = b.k").fetchone()[0]
        deletes_tombstoned = con.execute(
            "SELECT count(DISTINCT m.contract_transaction_unique_key) FROM merged m "
            "JOIN delete_keys d ON m.contract_transaction_unique_key = d.k").fetchone()[0]
        merged_rows = con.execute("SELECT count(*) FROM merged").fetchone()[0]
        dedup_collapsed = int(rows_in_bulk + rows_in_fresh + rows_in_archive_full - merged_rows)
        max_action_date = con.execute("SELECT max(action_date) FROM canonical_out").fetchone()[0]

        log(f"merged={merged_rows:,} rows_out={rows_out:,} fresh_only_tail={fresh_only_tail:,} "
            f"deletes_tombstoned={deletes_tombstoned:,} max_action_date={max_action_date}")

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
                    deletes_tombstoned=int(deletes_tombstoned), max_action_date=max_action_date,
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
    """§5 assertions on read-back. Independent scanner → DuckDB. Set-membership via ANTI JOIN
    (never NOT IN — NULL-poison). Returns JSON."""
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
    # DuckDB 1.5.4). Materialize because the six downstream queries multi-scan c, and a single-pass
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
    con.close()

    out = {
        "uri": target_uri,
        "rows_out": int(total),
        "pk_unique": bool(total == distinct),
        "pk_dupes": int(total - distinct),
        "null_pk_rows": int(null_pk),
        "max_action_date": str(mx_action) if mx_action is not None else None,
        "built_at_distinct": int(built_at_distinct),   # must be 1 (single injected literal)
        "canonical_source_distribution": {k: int(v) for k, v in src_dist.items()},
        "fresh_rows_with_enrichment": int(enrich_on_fresh),
        "columns": len(ds.schema.names),
        "indices": idx,
    }
    return out


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
