"""USAspending AWARD CANONICAL (award-summary spine) — typed v2.1 SoR reconciliation (GIANT).

Reconciles the contract-award-summary universe into ONE typed, generated_unique_award_id-grained
v2.1 Lance read-model (`s3://data-sink/active/usaspending_award_canonical/`, 393 typed columns):

    BULK    s3://data-sink/active/usaspending/award_search/            (154 typed matview cols; the
              contract-scope subset generated_unique_award_id LIKE 'CONT%' = ~30,683,126 rows)
    FRESH   s3://data-sink/active/usaspending_api_fresh/contract_prime_award/ (286 all-VARCHAR cols;
              the download/awards contract_prime_award overlay — 98,510 distinct awards)
    PARENT  s3://data-sink/active/usaspending/parent_award/            (987,705 rows, 1:1 on gua; a
              SEMI-JOIN-SCOPED 1:1 dimensional enrich leg — NOT a union member — supplying 10 net-new
              IDV rollup aggregates to the ~32,341 in-spine parents)

This is the AWARD-SUMMARY counterpart of usaspending_fpds_canonical.py (the FPDS transaction spine).
An award summary is the PARENT rollup of its FPDS transactions → SEPARATE canonical, PK-grained on
the single-column award key, never folded into the transaction table. Scope is CONTRACT-ONLY
(generated_unique_award_id LIKE 'CONT%'); assistance/grant summaries are a separate future canonical.

CANONICAL VOCABULARY = the FRESH download/awards PAS names (contract_award_unique_key, award_id_piid,
recipient_uei, …); BULK award_search matview names are crosswalked into that vocabulary via the DEC
two-hop (dl_award_element). BULK-only / FRESH-only / parent-rollup enrichment columns keep their
resolved canonical names. len(COLUMN_SPEC) == 393 (baked into smoke_fn / DDL columns / verify #3).

RECONCILIATION (BULK+FRESH per-key argmax, single-column PK; parent as a 1:1 dimensional enrich):
  • Single-column PK: generated_unique_award_id. LIVE-PROVEN (2026-07-04, s3://data-sink/active/):
    BULK is already 1:1 on gua IN CONTRACT SCOPE (30,419,755 rows = distinct gua = 0 null at the
    is_fpds probe; LIKE 'CONT%' widens to ~30,683,126) → the BULK collapse window is DEFENSIVE-ONLY
    (rn>1 never fires; dedup_collapsed from the BULK leg is expected ≈ 0 — do NOT flag near-zero
    BULK dedup as a bug). FRESH re-pulls duplicate keys across download windows → its collapse keeps
    the latest report per gua. PK-uniqueness is STRUCTURAL (one survivor per gua per source).
  • FRESH containment (LIVE): 98,510 distinct FRESH keys · 84,341 (85.6%) overlap with BULK under
    CONT% · fresh_only_tail 14,169. The include_fresh=TRUE flip MUST grow rows_out by the FRESH tail.
  • s() sentinel macro applied IDENTICALLY on both sources — whole-string ''/'-NONE-' → NULL.
  • bulk_latest / fresh_latest: EACH source collapsed latest-per-gua (row_number()=1, ORDER
    last_modified_date DESC NULLS LAST, then that source's own surrogate). Both projections are
    INLINED into their collapse windows (the ~30.4M bulk_proj is never materialized — spill hygiene).
  • core_winner: ONE flat 2-way window over the union of the two collapsed cores (source_rank
    FRESH(1) < BULK(2)) → argmax(last_modified_date); tie → FRESH. last_modified_date is the unified
    mod-frontier (BULK native TIMESTAMP vs FRESH replace('+00','')+TRY_CAST). NEVER sentinel-clamped.
  • parent_award leg — SEMI-JOIN SCOPED to the contract spine (P0-1). The full ~987,705-row
    parent_award is materialized into parent_r (small: 11 narrow cols, tens of MB — NOT a memory risk);
    the SQL semi-join then prunes it to the ~32,341 in-scope rows AT QUERY TIME, not at scan time
    (parent_award is 1:1 on gua; 96.7% is grant/assistance IDV rollups that never join → the WHERE gua
    IN (SELECT gua FROM bulk_latest) drops the dead 96.7%). NOT a collapse core, NOT a union member;
    participates ONLY in the stage-2 enrich LEFT JOIN.
    parent_award's own parent_award_id FK is NOT projected (96.6% NULL, drives nothing — P0-2).
  • Enrichment (SINGLE-SOURCE per column, independent of the core winner): 3-leg REPLACE — BULK-only
    enrich cols filled from bulk_latest (b.<col>); FRESH-only from fresh_latest (f.<col>); the 10
    parent-rollup cols (src="parent") from parent_latest (p.<col>). LEFT JOINs to gua-unique collapses
    → no fan-out. No COALESCE (each enrich has exactly one source).
  • canonical_source: derived ONCE as the winning core row's src tag (fresh|bulk) — the true per-key
    winner, never a partition literal.
  • NO delete/tombstone leg — award_search has no correction_delete_ind (154 cols, live-confirmed). NO
    monthly-CSV feed, NO archive snapshot leg. An award summary is superseded, never deleted.
  • Fail-closed gates raise BEFORE publish: (1) PK-uniqueness count(*)==distinct(gua); (2) rows_out
    floor = 0.90 × live BULK-scope (full-universe only); (3) tail-entered — include_fresh=TRUE ⇒
    rows_out > bulk_scope_rows (the fresh_only_tail must enter).

include_fresh TOGGLE (reconcile-later switch — data-driven emptiness, NOT a second SQL path):
  • include_fresh=TRUE  → fresh_r scanner OPENED (.to_table()); _fresh_collapse populates fresh_latest;
    canonical_source is a real {fresh,bulk} mix; the 14,169-key tail enters (rows_out grows).
  • include_fresh=FALSE → fresh_r scanner NOT opened; _fresh_collapse_empty projects the SAME canonical
    column list/order/type (every feed_expr → CAST(NULL AS <duck_type>)) with WHERE 1=0 → fresh_latest
    empty but schema-identical; core_union degenerates to BULK-only; every award resolves
    canonical_source='bulk'. FRESH-unique + dual columns land typed-NULL. The Arrow/Lance schema is
    BYTE-IDENTICAL either way — proven (not asserted) by smoke_fn's cross-toggle DESCRIBE check.

SENTINEL CLAMPS (value-level, row always survives; NEVER touch last_modified_date):
  • amounts (both legs): CASE WHEN abs(<expr>) <= 1e12 THEN <expr> END → the row survives, the garbage
    amount → NULL, so downstream SUM() is not poisoned. Emits identical DOUBLE on BULK-native and
    FRESH-TRY_CAST.
  • action dates (both legs): CASE WHEN <date> BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN <date>
    END → row survives, garbage date → NULL. NOT the argmax driver, so nulling never perturbs merge.

is_fpds NOTE (P1-7 / SCOPE CORRECTION): the contract scope is generated_unique_award_id LIKE 'CONT%',
NOT is_fpds=TRUE. is_fpds is NULL on 263,371 real CONT-prefix contracts (never FALSE for a contract);
is_fpds=TRUE would drop them and craters the FRESH reconcile (68,482 fresh awards whose BULK
counterpart exists would falsely read as fresh-only tail). CONTRACT_FILTER is pushed into the BULK
SCANNER filter, never the SQL body.

DISCIPLINES (d.8 / fleet rules — GIANT path, cloned from usaspending_fpds_canonical.py):
  • module-top os.environ.setdefault("LANCE_BYPASS_SPILLING","true") BEFORE any import lance.
  • NO direct-R2 write of the table (Giants 400 InvalidPart) — LOCAL Lance write → boto3 uniform-part
    publish; indices folded into the local dataset BEFORE the single atomic publish. RSS reclaim
    (del reader; gc; malloc_trim; rmtree spill) before the index sort. data_storage_version="2.1",
    max_rows_per_file=1048576 (valid ONLY on the boto3 path).
  • BULK → .to_reader() single-pass (registered bulk_r, consumed once by the inlined collapse;
    rows_in_bulk from m_rows_in_bulk). FRESH + parent → .to_table() (both small, re-scannable).
  • built_at = ONE Python naive-UTC literal injected into both projections (NOT now()).
  • last_modified_date parsed via replace(...,'+00','')+TRY_CAST (NO strptime hard-abort).
  • NO auto-retries in pipeline logic; overwrite idempotency.
  • --since pushes last_modified_date>= into BOTH data scanners (BULK naive TIMESTAMP; FRESH lexical
    ISO string). is_fpds/CONT% scope is scanner-side only, never the SQL body.

    # FULL contract-only build — GIANT, run on Modal (see usaspending_award_canonical_modal.py):
    modal run pipelines/usaspending/usaspending_award_canonical_modal.py::build --include-fresh false

    # SAMPLE (small --since slice — proves the merge + measures spill+stage footprint; on-box OK):
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'boto3>=1.35' --with 'psycopg[binary]>=3.2' \
      python3 -m pipelines.usaspending.usaspending_award_canonical build --since 2026-06-01 \
        --target-uri s3://data-sink/active/_sample/usaspending_award_canonical_sample/

    python3 -m pipelines.usaspending.usaspending_award_canonical init_ops
    python3 -m pipelines.usaspending.usaspending_award_canonical index  [--target-uri URI]
    python3 -m pipelines.usaspending.usaspending_award_canonical verify [--target-uri URI]
"""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import sys
from pathlib import Path

# In-RAM scalar-index sort — the small DataFusion external-merge pool OOMs ("ExternalSorterMerge")
# on a ~30M-row BTREE build. Set BEFORE any lance call (ARCHITECTURE.md fleet rule).
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

try:  # Modal is the production build substrate; the local CLI stays the dev/sample path.
    import modal
except ImportError:
    modal = None

BUCKET = "data-sink"
ACTIVE = "s3://data-sink/active"

BULK_URI = f"{ACTIVE}/usaspending/award_search/"
FRESH_URI = f"{ACTIVE}/usaspending_api_fresh/contract_prime_award/"
PARENT_URI = f"{ACTIVE}/usaspending/parent_award/"
CANONICAL_URI = f"{ACTIVE}/usaspending_award_canonical/"

# Contract-only scope. Pushed into the BULK SCANNER filter, NEVER the SQL body. is_fpds=TRUE is WRONG
# (NULL on 263,371 real CONT-prefix contracts + craters the FRESH reconcile — see module docstring
# SCOPE CORRECTION); LIKE 'CONT%' → ~30,683,126 rows.
CONTRACT_FILTER = "generated_unique_award_id LIKE 'CONT%'"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576          # valid ONLY on the boto3-publish path (uniform parts)
MAX_BYTES_PER_FILE = 90 * 1024**3

PK_COL = "generated_unique_award_id"

SCRATCH = os.environ.get("AWARD_CANONICAL_SCRATCH", "/tmp/award_canonical_stage")
DUCK_MEM = os.environ.get("AWARD_CANONICAL_DUCKDB_MEM", "8GB")
DUCK_TMP = os.environ.get("AWARD_CANONICAL_DUCKDB_TEMP_DIR", "/tmp/award_canonical_duckdb")
DUCK_THREADS = int(os.environ.get("AWARD_CANONICAL_DUCKDB_THREADS", "4"))

FEED = "usaspending_award_canonical"
OPS_SQL_FILE = "ops_usaspending_award_canonical_runs.sql"
OPS_TABLE = "ops.usaspending_award_canonical_runs"


# =========================================================================================== #
# THE COLUMN CONTRACT — the SINGLE source of truth. Every projection, the enrichment REPLACE
# block, the final column order, and the index lists are PROGRAM-GENERATED from this structure.
# Locked by the Phase-3 DEC-alignment pass (2026-07-04). len(COLUMN_SPEC) == 393.
#
# Each entry:
#   canonical : output column name (= FRESH download/awards PAS vocabulary; resolved names for enrich)
#   duck_type : DuckDB target type (DATE→date32, TIMESTAMP→naive timestamp[us], DOUBLE, BIGINT,
#               BOOLEAN, VARCHAR)
#   group     : 'key' | 'core' | 'enrich' | 'prov'
#   bulk_expr : BULK award_search projection expr (DEC-crosswalked). None ⇒ typed-NULL placeholder.
#   feed_expr : FRESH contract_prime_award projection expr. None ⇒ typed-NULL placeholder.
#   src       : "parent" ONLY on the 10 parent_award-dataset rows (both exprs None; filled via the
#               parent_latest enrich LEFT JOIN). Absent on every other row.
# Macro s(x) = nullif(nullif(trim(x),''),'-NONE-'). Typed cast: TRY_CAST(s(x) AS <T>).
# Mod-frontier: TRY_CAST(replace(s(x),'+00','') AS TIMESTAMP) (NO strptime). BULK native-typed
# (matview) → bare column refs; FRESH all-VARCHAR → always wrapped.
# core = DUAL-SOURCED (both legs). enrich = SINGLE-SOURCE (one leg None → typed NULL on the other, OR
# src="parent" with both None). prov = canonical_source (derived) + built_at (injected literal).
# =========================================================================================== #
COLUMN_SPEC: list[dict] = [
    # ---- group: key ----
    {"canonical": 'generated_unique_award_id', "duck_type": 'VARCHAR', "group": 'key', "bulk_expr": 's(generated_unique_award_id)', "feed_expr": 's(contract_award_unique_key)'},
    {"canonical": 'contract_award_unique_key', "duck_type": 'VARCHAR', "group": 'key', "bulk_expr": 's(generated_unique_award_id)', "feed_expr": 's(contract_award_unique_key)'},
    # ---- group: core ----
    {"canonical": 'last_modified_date', "duck_type": 'TIMESTAMP', "group": 'core', "bulk_expr": 'last_modified_date', "feed_expr": "TRY_CAST(replace(s(last_modified_date),'+00','') AS TIMESTAMP)"},
    {"canonical": 'total_obligation', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(total_obligation) <= 1e12 THEN total_obligation END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(total_obligated_amount) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(total_obligated_amount) AS DOUBLE) END'},
    {"canonical": 'base_and_all_options_value', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(base_and_all_options_value) <= 1e12 THEN base_and_all_options_value END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(potential_total_value_of_award) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(potential_total_value_of_award) AS DOUBLE) END'},
    {"canonical": 'product_or_service_description', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'product_or_service_description', "feed_expr": 's(product_or_service_code_description)'},
    {"canonical": 'naics_description', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'naics_description', "feed_expr": 's(naics_description)'},
    {"canonical": 'parent_award_id_piid', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'parent_award_piid', "feed_expr": 's(parent_award_id_piid)'},
    {"canonical": 'description', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'description', "feed_expr": 's(prime_award_base_transaction_description)'},
    {"canonical": 'action_date', "duck_type": 'DATE', "group": 'core', "bulk_expr": "CASE WHEN action_date BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN action_date END", "feed_expr": "CASE WHEN TRY_CAST(s(award_latest_action_date) AS DATE) BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN TRY_CAST(s(award_latest_action_date) AS DATE) END"},
    {"canonical": 'date_signed', "duck_type": 'DATE', "group": 'core', "bulk_expr": "CASE WHEN date_signed BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN date_signed END", "feed_expr": "CASE WHEN TRY_CAST(s(award_base_action_date) AS DATE) BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN TRY_CAST(s(award_base_action_date) AS DATE) END"},
    {"canonical": 'awarding_agency_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'awarding_toptier_agency_code', "feed_expr": 's(awarding_agency_code)'},
    {"canonical": 'awarding_agency_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'awarding_toptier_agency_name', "feed_expr": 's(awarding_agency_name)'},
    {"canonical": 'awarding_sub_agency_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'awarding_subtier_agency_code', "feed_expr": 's(awarding_sub_agency_code)'},
    {"canonical": 'awarding_sub_agency_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'awarding_subtier_agency_name', "feed_expr": 's(awarding_sub_agency_name)'},
    {"canonical": 'funding_agency_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'funding_toptier_agency_code', "feed_expr": 's(funding_agency_code)'},
    {"canonical": 'funding_agency_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'funding_toptier_agency_name', "feed_expr": 's(funding_agency_name)'},
    {"canonical": 'funding_sub_agency_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'funding_subtier_agency_code', "feed_expr": 's(funding_sub_agency_code)'},
    {"canonical": 'funding_sub_agency_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'funding_subtier_agency_name', "feed_expr": 's(funding_sub_agency_name)'},
    {"canonical": 'recipient_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_name', "feed_expr": 's(recipient_name)'},
    {"canonical": 'recipient_parent_uei', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'parent_uei', "feed_expr": 's(recipient_parent_uei)'},
    {"canonical": 'recipient_country_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_country_code', "feed_expr": 's(recipient_country_code)'},
    {"canonical": 'recipient_country_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_country_name', "feed_expr": 's(recipient_country_name)'},
    {"canonical": 'recipient_state_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_state_code', "feed_expr": 's(recipient_state_code)'},
    {"canonical": 'recipient_state_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_state_name', "feed_expr": 's(recipient_state_name)'},
    {"canonical": 'recipient_county_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_county_name', "feed_expr": 's(recipient_county_name)'},
    {"canonical": 'recipient_city_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_city_name', "feed_expr": 's(recipient_city_name)'},
    {"canonical": 'prime_award_summary_recipient_cd_current', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_location_congressional_code_current', "feed_expr": 's(prime_award_summary_recipient_cd_current)'},
    {"canonical": 'primary_place_of_performance_country_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_country_code', "feed_expr": 's(primary_place_of_performance_country_code)'},
    {"canonical": 'primary_place_of_performance_country_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_country_name', "feed_expr": 's(primary_place_of_performance_country_name)'},
    {"canonical": 'primary_place_of_performance_state_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_state_code', "feed_expr": 's(primary_place_of_performance_state_code)'},
    {"canonical": 'primary_place_of_performance_state_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_state_name', "feed_expr": 's(primary_place_of_performance_state_name)'},
    {"canonical": 'primary_place_of_performance_county_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_county_name', "feed_expr": 's(primary_place_of_performance_county_name)'},
    {"canonical": 'primary_place_of_performance_city_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_city_name', "feed_expr": 's(primary_place_of_performance_city_name)'},
    {"canonical": 'prime_award_summary_place_of_performance_cd_current', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'pop_congressional_code_current', "feed_expr": 's(prime_award_summary_place_of_performance_cd_current)'},
    {"canonical": 'award_type_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'type', "feed_expr": 's(award_type_code)'},
    {"canonical": 'award_type', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'type_description', "feed_expr": 's(award_type)'},
    {"canonical": 'disaster_emergency_fund_codes', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'disaster_emergency_fund_codes', "feed_expr": 's(disaster_emergency_fund_codes)'},
    {"canonical": 'federal_accounts_funding_this_award', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'federal_accounts', "feed_expr": 's(federal_accounts_funding_this_award)'},
    {"canonical": 'program_activities_funding_this_award', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'program_activities', "feed_expr": 's(program_activities_funding_this_award)'},
    {"canonical": 'extent_competed', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'extent_competed', "feed_expr": 's(extent_competed)'},
    {"canonical": 'naics_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'naics_code', "feed_expr": 's(naics_code)'},
    {"canonical": 'highly_compensated_officer_1_amount', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(officer_1_amount) <= 1e12 THEN officer_1_amount END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(highly_compensated_officer_1_amount) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(highly_compensated_officer_1_amount) AS DOUBLE) END'},
    {"canonical": 'highly_compensated_officer_1_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'officer_1_name', "feed_expr": 's(highly_compensated_officer_1_name)'},
    {"canonical": 'highly_compensated_officer_2_amount', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(officer_2_amount) <= 1e12 THEN officer_2_amount END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(highly_compensated_officer_2_amount) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(highly_compensated_officer_2_amount) AS DOUBLE) END'},
    {"canonical": 'highly_compensated_officer_2_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'officer_2_name', "feed_expr": 's(highly_compensated_officer_2_name)'},
    {"canonical": 'highly_compensated_officer_3_amount', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(officer_3_amount) <= 1e12 THEN officer_3_amount END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(highly_compensated_officer_3_amount) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(highly_compensated_officer_3_amount) AS DOUBLE) END'},
    {"canonical": 'highly_compensated_officer_3_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'officer_3_name', "feed_expr": 's(highly_compensated_officer_3_name)'},
    {"canonical": 'highly_compensated_officer_4_amount', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(officer_4_amount) <= 1e12 THEN officer_4_amount END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(highly_compensated_officer_4_amount) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(highly_compensated_officer_4_amount) AS DOUBLE) END'},
    {"canonical": 'highly_compensated_officer_4_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'officer_4_name', "feed_expr": 's(highly_compensated_officer_4_name)'},
    {"canonical": 'highly_compensated_officer_5_amount', "duck_type": 'DOUBLE', "group": 'core', "bulk_expr": 'CASE WHEN abs(officer_5_amount) <= 1e12 THEN officer_5_amount END', "feed_expr": 'CASE WHEN abs(TRY_CAST(s(highly_compensated_officer_5_amount) AS DOUBLE)) <= 1e12 THEN TRY_CAST(s(highly_compensated_officer_5_amount) AS DOUBLE) END'},
    {"canonical": 'highly_compensated_officer_5_name', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'officer_5_name', "feed_expr": 's(highly_compensated_officer_5_name)'},
    {"canonical": 'ordering_period_end_date', "duck_type": 'DATE', "group": 'core', "bulk_expr": "CASE WHEN ordering_period_end_date BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN ordering_period_end_date END", "feed_expr": "CASE WHEN TRY_CAST(s(ordering_period_end_date) AS DATE) BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN TRY_CAST(s(ordering_period_end_date) AS DATE) END"},
    {"canonical": 'period_of_performance_current_end_date', "duck_type": 'DATE', "group": 'core', "bulk_expr": "CASE WHEN period_of_performance_current_end_date BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN period_of_performance_current_end_date END", "feed_expr": "CASE WHEN TRY_CAST(s(period_of_performance_current_end_date) AS DATE) BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN TRY_CAST(s(period_of_performance_current_end_date) AS DATE) END"},
    {"canonical": 'period_of_performance_start_date', "duck_type": 'DATE', "group": 'core', "bulk_expr": "CASE WHEN period_of_performance_start_date BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN period_of_performance_start_date END", "feed_expr": "CASE WHEN TRY_CAST(s(period_of_performance_start_date) AS DATE) BETWEEN DATE '1776-01-01' AND CURRENT_DATE THEN TRY_CAST(s(period_of_performance_start_date) AS DATE) END"},
    {"canonical": 'award_id_piid', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'piid', "feed_expr": 's(award_id_piid)'},
    {"canonical": 'product_or_service_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'product_or_service_code', "feed_expr": 's(product_or_service_code)'},
    {"canonical": 'recipient_uei', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'recipient_uei', "feed_expr": 's(recipient_uei)'},
    {"canonical": 'type_of_contract_pricing', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'type_of_contract_pricing', "feed_expr": 's(type_of_contract_pricing)'},
    {"canonical": 'type_of_set_aside_code', "duck_type": 'VARCHAR', "group": 'core', "bulk_expr": 'type_set_aside', "feed_expr": 's(type_of_set_aside_code)'},
    # ---- group: enrich ----
    # -- enrich: bulk-unique --
    {"canonical": 'treasury_account_identifiers', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'treasury_account_identifiers', "feed_expr": None},
    {"canonical": 'award_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'award_id', "feed_expr": None},
    {"canonical": 'category', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(category)', "feed_expr": None},
    {"canonical": 'display_award_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(display_award_id)', "feed_expr": None},
    {"canonical": 'update_date', "duck_type": 'TIMESTAMP', "group": 'enrich', "bulk_expr": 'update_date', "feed_expr": None},
    {"canonical": 'fain', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(fain)', "feed_expr": None},
    {"canonical": 'uri', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(uri)', "feed_expr": None},
    {"canonical": 'award_amount', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'award_amount', "feed_expr": None},
    {"canonical": 'total_subsidy_cost', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_subsidy_cost', "feed_expr": None},
    {"canonical": 'total_loan_value', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_loan_value', "feed_expr": None},
    {"canonical": 'total_obl_bin', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(total_obl_bin)', "feed_expr": None},
    {"canonical": 'recipient_hash', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_hash)', "feed_expr": None},
    {"canonical": 'recipient_levels', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_levels)', "feed_expr": None},
    {"canonical": 'recipient_unique_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_unique_id)', "feed_expr": None},
    {"canonical": 'parent_recipient_unique_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(parent_recipient_unique_id)', "feed_expr": None},
    {"canonical": 'business_categories', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(business_categories)', "feed_expr": None},
    {"canonical": 'fiscal_year', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'fiscal_year', "feed_expr": None},
    {"canonical": 'original_loan_subsidy_cost', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'original_loan_subsidy_cost', "feed_expr": None},
    {"canonical": 'face_value_loan_guarantee', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'face_value_loan_guarantee', "feed_expr": None},
    {"canonical": 'awarding_agency_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'awarding_agency_id', "feed_expr": None},
    {"canonical": 'funding_agency_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'funding_agency_id', "feed_expr": None},
    {"canonical": 'funding_toptier_agency_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'funding_toptier_agency_id', "feed_expr": None},
    {"canonical": 'funding_subtier_agency_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'funding_subtier_agency_id', "feed_expr": None},
    {"canonical": 'recipient_location_county_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_county_code)', "feed_expr": None},
    {"canonical": 'recipient_location_zip5', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_zip5)', "feed_expr": None},
    {"canonical": 'recipient_location_congressional_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_congressional_code)', "feed_expr": None},
    {"canonical": 'recipient_location_state_fips', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_state_fips)', "feed_expr": None},
    {"canonical": 'recipient_location_state_population', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'recipient_location_state_population', "feed_expr": None},
    {"canonical": 'recipient_location_county_population', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'recipient_location_county_population', "feed_expr": None},
    {"canonical": 'recipient_location_congressional_population', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'recipient_location_congressional_population', "feed_expr": None},
    {"canonical": 'pop_county_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_county_code)', "feed_expr": None},
    {"canonical": 'pop_city_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_city_code)', "feed_expr": None},
    {"canonical": 'pop_zip5', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_zip5)', "feed_expr": None},
    {"canonical": 'pop_congressional_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_congressional_code)', "feed_expr": None},
    {"canonical": 'pop_state_fips', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_state_fips)', "feed_expr": None},
    {"canonical": 'pop_state_population', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'pop_state_population', "feed_expr": None},
    {"canonical": 'pop_county_population', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'pop_county_population', "feed_expr": None},
    {"canonical": 'pop_congressional_population', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'pop_congressional_population', "feed_expr": None},
    {"canonical": 'cfda_program_title', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(cfda_program_title)', "feed_expr": None},
    {"canonical": 'cfda_number', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(cfda_number)', "feed_expr": None},
    {"canonical": 'cfdas', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(cfdas)', "feed_expr": None},
    {"canonical": 'sai_number', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(sai_number)', "feed_expr": None},
    {"canonical": 'tas_paths', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(tas_paths)', "feed_expr": None},
    {"canonical": 'tas_components', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(tas_components)', "feed_expr": None},
    {"canonical": 'total_covid_outlay', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_covid_outlay', "feed_expr": None},
    {"canonical": 'total_covid_obligation', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_covid_obligation', "feed_expr": None},
    {"canonical": 'base_exercised_options_val', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'base_exercised_options_val', "feed_expr": None},
    {"canonical": 'certified_date', "duck_type": 'DATE', "group": 'enrich', "bulk_expr": 'certified_date', "feed_expr": None},
    {"canonical": 'create_date', "duck_type": 'TIMESTAMP', "group": 'enrich', "bulk_expr": 'create_date', "feed_expr": None},
    {"canonical": 'fpds_agency_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(fpds_agency_id)', "feed_expr": None},
    {"canonical": 'fpds_parent_agency_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(fpds_parent_agency_id)', "feed_expr": None},
    {"canonical": 'non_federal_funding_amount', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'non_federal_funding_amount', "feed_expr": None},
    {"canonical": 'raw_recipient_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(raw_recipient_name)', "feed_expr": None},
    {"canonical": 'subaward_count', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'subaward_count', "feed_expr": None},
    {"canonical": 'total_funding_amount', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_funding_amount', "feed_expr": None},
    {"canonical": 'total_indirect_federal_sharing', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_indirect_federal_sharing', "feed_expr": None},
    {"canonical": 'total_subaward_amount', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_subaward_amount', "feed_expr": None},
    {"canonical": 'transaction_unique_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(transaction_unique_id)', "feed_expr": None},
    {"canonical": 'awarding_subtier_agency_code_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(awarding_subtier_agency_code_raw)', "feed_expr": None},
    {"canonical": 'awarding_subtier_agency_name_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(awarding_subtier_agency_name_raw)', "feed_expr": None},
    {"canonical": 'awarding_toptier_agency_code_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(awarding_toptier_agency_code_raw)', "feed_expr": None},
    {"canonical": 'awarding_toptier_agency_name_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(awarding_toptier_agency_name_raw)', "feed_expr": None},
    {"canonical": 'funding_subtier_agency_code_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(funding_subtier_agency_code_raw)', "feed_expr": None},
    {"canonical": 'funding_subtier_agency_name_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(funding_subtier_agency_name_raw)', "feed_expr": None},
    {"canonical": 'funding_toptier_agency_code_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(funding_toptier_agency_code_raw)', "feed_expr": None},
    {"canonical": 'funding_toptier_agency_name_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(funding_toptier_agency_name_raw)', "feed_expr": None},
    {"canonical": 'data_source', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(data_source)', "feed_expr": None},
    {"canonical": 'earliest_transaction_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'earliest_transaction_id', "feed_expr": None},
    {"canonical": 'latest_transaction_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'latest_transaction_id', "feed_expr": None},
    {"canonical": 'earliest_transaction_search_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'earliest_transaction_search_id', "feed_expr": None},
    {"canonical": 'latest_transaction_search_id', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'latest_transaction_search_id', "feed_expr": None},
    {"canonical": 'total_iija_obligation', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_iija_obligation', "feed_expr": None},
    {"canonical": 'total_iija_outlay', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_iija_outlay', "feed_expr": None},
    {"canonical": 'total_outlays', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'total_outlays', "feed_expr": None},
    {"canonical": 'pop_county_fips', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_county_fips)', "feed_expr": None},
    {"canonical": 'recipient_location_county_fips', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_county_fips)', "feed_expr": None},
    {"canonical": 'type_description_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(type_description_raw)', "feed_expr": None},
    {"canonical": 'type_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(type_raw)', "feed_expr": None},
    {"canonical": 'parent_recipient_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(parent_recipient_name)', "feed_expr": None},
    {"canonical": 'generated_pragmatic_obligation', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": 'generated_pragmatic_obligation', "feed_expr": None},
    {"canonical": 'pop_zip4', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(pop_zip4)', "feed_expr": None},
    {"canonical": 'recipient_location_address_line1', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_address_line1)', "feed_expr": None},
    {"canonical": 'recipient_location_address_line2', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_address_line2)', "feed_expr": None},
    {"canonical": 'recipient_location_address_line3', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_address_line3)', "feed_expr": None},
    {"canonical": 'recipient_location_foreign_postal_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_foreign_postal_code)', "feed_expr": None},
    {"canonical": 'recipient_location_foreign_province', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_foreign_province)', "feed_expr": None},
    {"canonical": 'recipient_location_zip4', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(recipient_location_zip4)', "feed_expr": None},
    {"canonical": 'generated_unique_award_id_legacy', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(generated_unique_award_id_legacy)', "feed_expr": None},
    {"canonical": 'spending_by_defc', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(spending_by_defc)', "feed_expr": None},
    {"canonical": 'transaction_count', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": 'transaction_count', "feed_expr": None},
    {"canonical": 'source_schema', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(source_schema)', "feed_expr": None},
    {"canonical": 'source_table', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": 's(source_table)', "feed_expr": None},
    {"canonical": 'usaspending_snapshot_date', "duck_type": 'DATE', "group": 'enrich', "bulk_expr": 'usaspending_snapshot_date', "feed_expr": None},
    {"canonical": 'ingested_at', "duck_type": 'TIMESTAMP', "group": 'enrich', "bulk_expr": 'CAST(ingested_at AS TIMESTAMP)', "feed_expr": None},
    # -- enrich: fresh-unique --
    {"canonical": 'parent_award_agency_id', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(parent_award_agency_id)'},
    {"canonical": 'parent_award_agency_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(parent_award_agency_name)'},
    {"canonical": 'outlayed_amount_from_COVID_19_supplementals', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s("outlayed_amount_from_COVID-19_supplementals") AS DOUBLE)'},
    {"canonical": 'obligated_amount_from_COVID_19_supplementals', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s("obligated_amount_from_COVID-19_supplementals") AS DOUBLE)'},
    {"canonical": 'outlayed_amount_from_IIJA_supplemental', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(outlayed_amount_from_IIJA_supplemental) AS DOUBLE)'},
    {"canonical": 'obligated_amount_from_IIJA_supplemental', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(obligated_amount_from_IIJA_supplemental) AS DOUBLE)'},
    {"canonical": 'total_outlayed_amount', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(total_outlayed_amount) AS DOUBLE)'},
    {"canonical": 'current_total_value_of_award', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(current_total_value_of_award) AS DOUBLE)'},
    {"canonical": 'award_base_action_date_fiscal_year', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(award_base_action_date_fiscal_year) AS BIGINT)'},
    {"canonical": 'award_latest_action_date_fiscal_year', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(award_latest_action_date_fiscal_year) AS BIGINT)'},
    {"canonical": 'period_of_performance_potential_end_date', "duck_type": 'DATE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(period_of_performance_potential_end_date) AS DATE)'},
    {"canonical": 'solicitation_date', "duck_type": 'DATE', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(solicitation_date) AS DATE)'},
    {"canonical": 'awarding_office_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(awarding_office_code)'},
    {"canonical": 'awarding_office_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(awarding_office_name)'},
    {"canonical": 'funding_office_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(funding_office_code)'},
    {"canonical": 'funding_office_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(funding_office_name)'},
    {"canonical": 'treasury_accounts_funding_this_award', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(treasury_accounts_funding_this_award)'},
    {"canonical": 'object_classes_funding_this_award', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(object_classes_funding_this_award)'},
    {"canonical": 'foreign_funding', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(foreign_funding)'},
    {"canonical": 'foreign_funding_description', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(foreign_funding_description)'},
    {"canonical": 'sam_exception', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(sam_exception)'},
    {"canonical": 'sam_exception_description', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(sam_exception_description)'},
    {"canonical": 'recipient_duns', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_duns)'},
    {"canonical": 'recipient_name_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_name_raw)'},
    {"canonical": 'recipient_doing_business_as_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_doing_business_as_name)'},
    {"canonical": 'cage_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(cage_code)'},
    {"canonical": 'recipient_parent_duns', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_parent_duns)'},
    {"canonical": 'recipient_parent_name', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_parent_name)'},
    {"canonical": 'recipient_parent_name_raw', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_parent_name_raw)'},
    {"canonical": 'recipient_address_line_1', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_address_line_1)'},
    {"canonical": 'recipient_address_line_2', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_address_line_2)'},
    {"canonical": 'prime_award_summary_recipient_county_fips_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(prime_award_summary_recipient_county_fips_code)'},
    {"canonical": 'prime_award_summary_recipient_state_fips_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(prime_award_summary_recipient_state_fips_code)'},
    {"canonical": 'recipient_zip_4_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_zip_4_code)'},
    {"canonical": 'prime_award_summary_recipient_cd_original', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(prime_award_summary_recipient_cd_original)'},
    {"canonical": 'recipient_phone_number', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_phone_number)'},
    {"canonical": 'recipient_fax_number', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recipient_fax_number)'},
    {"canonical": 'prime_award_summary_place_of_performance_county_fips_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(prime_award_summary_place_of_performance_county_fips_code)'},
    {"canonical": 'prime_award_summary_place_of_performance_state_fips_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(prime_award_summary_place_of_performance_state_fips_code)'},
    {"canonical": 'primary_place_of_performance_zip_4', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(primary_place_of_performance_zip_4)'},
    {"canonical": 'prime_award_summary_place_of_performance_cd_original', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(prime_award_summary_place_of_performance_cd_original)'},
    {"canonical": 'award_or_idv_flag', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(award_or_idv_flag)'},
    {"canonical": 'idv_type_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(idv_type_code)'},
    {"canonical": 'idv_type', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(idv_type)'},
    {"canonical": 'multiple_or_single_award_idv_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(multiple_or_single_award_idv_code)'},
    {"canonical": 'multiple_or_single_award_idv', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(multiple_or_single_award_idv)'},
    {"canonical": 'type_of_idc_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(type_of_idc_code)'},
    {"canonical": 'type_of_idc', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(type_of_idc)'},
    {"canonical": 'type_of_contract_pricing_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(type_of_contract_pricing_code)'},
    {"canonical": 'solicitation_identifier', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(solicitation_identifier)'},
    {"canonical": 'number_of_actions', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(number_of_actions) AS BIGINT)'},
    {"canonical": 'inherently_governmental_functions', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(inherently_governmental_functions)'},
    {"canonical": 'inherently_governmental_functions_description', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(inherently_governmental_functions_description)'},
    {"canonical": 'contract_bundling_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contract_bundling_code)'},
    {"canonical": 'contract_bundling', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contract_bundling)'},
    {"canonical": 'dod_claimant_program_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(dod_claimant_program_code)'},
    {"canonical": 'dod_claimant_program_description', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(dod_claimant_program_description)'},
    {"canonical": 'recovered_materials_sustainability_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recovered_materials_sustainability_code)'},
    {"canonical": 'recovered_materials_sustainability', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(recovered_materials_sustainability)'},
    {"canonical": 'domestic_or_foreign_entity_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(domestic_or_foreign_entity_code)'},
    {"canonical": 'domestic_or_foreign_entity', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(domestic_or_foreign_entity)'},
    {"canonical": 'dod_acquisition_program_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(dod_acquisition_program_code)'},
    {"canonical": 'dod_acquisition_program_description', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(dod_acquisition_program_description)'},
    {"canonical": 'information_technology_commercial_item_category_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(information_technology_commercial_item_category_code)'},
    {"canonical": 'information_technology_commercial_item_category', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(information_technology_commercial_item_category)'},
    {"canonical": 'epa_designated_product_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(epa_designated_product_code)'},
    {"canonical": 'epa_designated_product', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(epa_designated_product)'},
    {"canonical": 'country_of_product_or_service_origin_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(country_of_product_or_service_origin_code)'},
    {"canonical": 'country_of_product_or_service_origin', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(country_of_product_or_service_origin)'},
    {"canonical": 'place_of_manufacture_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(place_of_manufacture_code)'},
    {"canonical": 'place_of_manufacture', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(place_of_manufacture)'},
    {"canonical": 'subcontracting_plan_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(subcontracting_plan_code)'},
    {"canonical": 'subcontracting_plan', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(subcontracting_plan)'},
    {"canonical": 'extent_competed_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(extent_competed_code)'},
    {"canonical": 'solicitation_procedures_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(solicitation_procedures_code)'},
    {"canonical": 'solicitation_procedures', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(solicitation_procedures)'},
    {"canonical": 'type_of_set_aside', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(type_of_set_aside)'},
    {"canonical": 'evaluated_preference_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(evaluated_preference_code)'},
    {"canonical": 'evaluated_preference', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(evaluated_preference)'},
    {"canonical": 'research_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(research_code)'},
    {"canonical": 'research', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(research)'},
    {"canonical": 'fair_opportunity_limited_sources_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(fair_opportunity_limited_sources_code)'},
    {"canonical": 'fair_opportunity_limited_sources', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(fair_opportunity_limited_sources)'},
    {"canonical": 'other_than_full_and_open_competition_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(other_than_full_and_open_competition_code)'},
    {"canonical": 'other_than_full_and_open_competition', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(other_than_full_and_open_competition)'},
    {"canonical": 'number_of_offers_received', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": 'TRY_CAST(s(number_of_offers_received) AS BIGINT)'},
    {"canonical": 'commercial_item_acquisition_procedures_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(commercial_item_acquisition_procedures_code)'},
    {"canonical": 'commercial_item_acquisition_procedures', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(commercial_item_acquisition_procedures)'},
    {"canonical": 'small_business_competitiveness_demonstration_program', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(small_business_competitiveness_demonstration_program)'},
    {"canonical": 'simplified_procedures_for_certain_commercial_items_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(simplified_procedures_for_certain_commercial_items_code)'},
    {"canonical": 'simplified_procedures_for_certain_commercial_items', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(simplified_procedures_for_certain_commercial_items)'},
    {"canonical": 'a76_fair_act_action_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(a76_fair_act_action_code)'},
    {"canonical": 'a76_fair_act_action', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(a76_fair_act_action)'},
    {"canonical": 'fed_biz_opps_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(fed_biz_opps_code)'},
    {"canonical": 'fed_biz_opps', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(fed_biz_opps)'},
    {"canonical": 'local_area_set_aside_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(local_area_set_aside_code)'},
    {"canonical": 'local_area_set_aside', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(local_area_set_aside)'},
    {"canonical": 'price_evaluation_adjustment_preference_percent_difference', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(price_evaluation_adjustment_preference_percent_difference)'},
    {"canonical": 'clinger_cohen_act_planning_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(clinger_cohen_act_planning_code)'},
    {"canonical": 'clinger_cohen_act_planning', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(clinger_cohen_act_planning)'},
    {"canonical": 'materials_supplies_articles_equipment_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(materials_supplies_articles_equipment_code)'},
    {"canonical": 'materials_supplies_articles_equipment', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(materials_supplies_articles_equipment)'},
    {"canonical": 'labor_standards_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(labor_standards_code)'},
    {"canonical": 'labor_standards', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(labor_standards)'},
    {"canonical": 'construction_wage_rate_requirements_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(construction_wage_rate_requirements_code)'},
    {"canonical": 'construction_wage_rate_requirements', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(construction_wage_rate_requirements)'},
    {"canonical": 'interagency_contracting_authority_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(interagency_contracting_authority_code)'},
    {"canonical": 'interagency_contracting_authority', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(interagency_contracting_authority)'},
    {"canonical": 'other_statutory_authority', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(other_statutory_authority)'},
    {"canonical": 'program_acronym', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(program_acronym)'},
    {"canonical": 'parent_award_type_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(parent_award_type_code)'},
    {"canonical": 'parent_award_type', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(parent_award_type)'},
    {"canonical": 'parent_award_single_or_multiple_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(parent_award_single_or_multiple_code)'},
    {"canonical": 'parent_award_single_or_multiple', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(parent_award_single_or_multiple)'},
    {"canonical": 'major_program', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(major_program)'},
    {"canonical": 'national_interest_action_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(national_interest_action_code)'},
    {"canonical": 'national_interest_action', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(national_interest_action)'},
    {"canonical": 'cost_or_pricing_data_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(cost_or_pricing_data_code)'},
    {"canonical": 'cost_or_pricing_data', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(cost_or_pricing_data)'},
    {"canonical": 'cost_accounting_standards_clause_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(cost_accounting_standards_clause_code)'},
    {"canonical": 'cost_accounting_standards_clause', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(cost_accounting_standards_clause)'},
    {"canonical": 'government_furnished_property_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(government_furnished_property_code)'},
    {"canonical": 'government_furnished_property', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(government_furnished_property)'},
    {"canonical": 'sea_transportation_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(sea_transportation_code)'},
    {"canonical": 'sea_transportation', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(sea_transportation)'},
    {"canonical": 'consolidated_contract_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(consolidated_contract_code)'},
    {"canonical": 'consolidated_contract', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(consolidated_contract)'},
    {"canonical": 'performance_based_service_acquisition_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(performance_based_service_acquisition_code)'},
    {"canonical": 'performance_based_service_acquisition', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(performance_based_service_acquisition)'},
    {"canonical": 'multi_year_contract_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(multi_year_contract_code)'},
    {"canonical": 'multi_year_contract', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(multi_year_contract)'},
    {"canonical": 'contract_financing_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contract_financing_code)'},
    {"canonical": 'contract_financing', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contract_financing)'},
    {"canonical": 'purchase_card_as_payment_method_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(purchase_card_as_payment_method_code)'},
    {"canonical": 'purchase_card_as_payment_method', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(purchase_card_as_payment_method)'},
    {"canonical": 'contingency_humanitarian_or_peacekeeping_operation_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contingency_humanitarian_or_peacekeeping_operation_code)'},
    {"canonical": 'contingency_humanitarian_or_peacekeeping_operation', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contingency_humanitarian_or_peacekeeping_operation)'},
    {"canonical": 'alaskan_native_corporation_owned_firm', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(alaskan_native_corporation_owned_firm)'},
    {"canonical": 'american_indian_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(american_indian_owned_business)'},
    {"canonical": 'indian_tribe_federally_recognized', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(indian_tribe_federally_recognized)'},
    {"canonical": 'native_hawaiian_organization_owned_firm', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(native_hawaiian_organization_owned_firm)'},
    {"canonical": 'tribally_owned_firm', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(tribally_owned_firm)'},
    {"canonical": 'veteran_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(veteran_owned_business)'},
    {"canonical": 'service_disabled_veteran_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(service_disabled_veteran_owned_business)'},
    {"canonical": 'woman_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(woman_owned_business)'},
    {"canonical": 'women_owned_small_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(women_owned_small_business)'},
    {"canonical": 'economically_disadvantaged_women_owned_small_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(economically_disadvantaged_women_owned_small_business)'},
    {"canonical": 'joint_venture_women_owned_small_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(joint_venture_women_owned_small_business)'},
    {"canonical": 'joint_venture_economic_disadvantaged_women_owned_small_bus', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(joint_venture_economic_disadvantaged_women_owned_small_bus)'},
    {"canonical": 'minority_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(minority_owned_business)'},
    {"canonical": 'subcontinent_asian_asian_indian_american_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(subcontinent_asian_asian_indian_american_owned_business)'},
    {"canonical": 'asian_pacific_american_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(asian_pacific_american_owned_business)'},
    {"canonical": 'black_american_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(black_american_owned_business)'},
    {"canonical": 'hispanic_american_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(hispanic_american_owned_business)'},
    {"canonical": 'native_american_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(native_american_owned_business)'},
    {"canonical": 'other_minority_owned_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(other_minority_owned_business)'},
    {"canonical": 'contracting_officers_determination_of_business_size', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contracting_officers_determination_of_business_size)'},
    {"canonical": 'contracting_officers_determination_of_business_size_code', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(contracting_officers_determination_of_business_size_code)'},
    {"canonical": 'emerging_small_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(emerging_small_business)'},
    {"canonical": 'community_developed_corporation_owned_firm', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(community_developed_corporation_owned_firm)'},
    {"canonical": 'labor_surplus_area_firm', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(labor_surplus_area_firm)'},
    {"canonical": 'us_federal_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(us_federal_government)'},
    {"canonical": 'federally_funded_research_and_development_corp', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(federally_funded_research_and_development_corp)'},
    {"canonical": 'federal_agency', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(federal_agency)'},
    {"canonical": 'us_state_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(us_state_government)'},
    {"canonical": 'us_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(us_local_government)'},
    {"canonical": 'city_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(city_local_government)'},
    {"canonical": 'county_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(county_local_government)'},
    {"canonical": 'inter_municipal_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(inter_municipal_local_government)'},
    {"canonical": 'local_government_owned', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(local_government_owned)'},
    {"canonical": 'municipality_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(municipality_local_government)'},
    {"canonical": 'school_district_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(school_district_local_government)'},
    {"canonical": 'township_local_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(township_local_government)'},
    {"canonical": 'us_tribal_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(us_tribal_government)'},
    {"canonical": 'foreign_government', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(foreign_government)'},
    {"canonical": 'organizational_type', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(organizational_type)'},
    {"canonical": 'corporate_entity_not_tax_exempt', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(corporate_entity_not_tax_exempt)'},
    {"canonical": 'corporate_entity_tax_exempt', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(corporate_entity_tax_exempt)'},
    {"canonical": 'partnership_or_limited_liability_partnership', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(partnership_or_limited_liability_partnership)'},
    {"canonical": 'sole_proprietorship', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(sole_proprietorship)'},
    {"canonical": 'small_agricultural_cooperative', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(small_agricultural_cooperative)'},
    {"canonical": 'international_organization', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(international_organization)'},
    {"canonical": 'us_government_entity', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(us_government_entity)'},
    {"canonical": 'community_development_corporation', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(community_development_corporation)'},
    {"canonical": 'domestic_shelter', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(domestic_shelter)'},
    {"canonical": 'educational_institution', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(educational_institution)'},
    {"canonical": 'foundation', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(foundation)'},
    {"canonical": 'hospital_flag', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(hospital_flag)'},
    {"canonical": 'manufacturer_of_goods', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(manufacturer_of_goods)'},
    {"canonical": 'veterinary_hospital', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(veterinary_hospital)'},
    {"canonical": 'hispanic_servicing_institution', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(hispanic_servicing_institution)'},
    {"canonical": 'receives_contracts', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(receives_contracts)'},
    {"canonical": 'receives_financial_assistance', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(receives_financial_assistance)'},
    {"canonical": 'receives_contracts_and_financial_assistance', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(receives_contracts_and_financial_assistance)'},
    {"canonical": 'airport_authority', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(airport_authority)'},
    {"canonical": 'council_of_governments', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(council_of_governments)'},
    {"canonical": 'housing_authorities_public_tribal', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(housing_authorities_public_tribal)'},
    {"canonical": 'interstate_entity', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(interstate_entity)'},
    {"canonical": 'planning_commission', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(planning_commission)'},
    {"canonical": 'port_authority', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(port_authority)'},
    {"canonical": 'transit_authority', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(transit_authority)'},
    {"canonical": 'subchapter_scorporation', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(subchapter_scorporation)'},
    {"canonical": 'limited_liability_corporation', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(limited_liability_corporation)'},
    {"canonical": 'foreign_owned', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(foreign_owned)'},
    {"canonical": 'for_profit_organization', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(for_profit_organization)'},
    {"canonical": 'nonprofit_organization', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(nonprofit_organization)'},
    {"canonical": 'other_not_for_profit_organization', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(other_not_for_profit_organization)'},
    {"canonical": 'the_ability_one_program', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(the_ability_one_program)'},
    {"canonical": 'private_university_or_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(private_university_or_college)'},
    {"canonical": 'state_controlled_institution_of_higher_learning', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(state_controlled_institution_of_higher_learning)'},
    {"canonical": 'c1862_land_grant_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's("1862_land_grant_college")'},
    {"canonical": 'c1890_land_grant_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's("1890_land_grant_college")'},
    {"canonical": 'c1994_land_grant_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's("1994_land_grant_college")'},
    {"canonical": 'minority_institution', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(minority_institution)'},
    {"canonical": 'historically_black_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(historically_black_college)'},
    {"canonical": 'tribal_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(tribal_college)'},
    {"canonical": 'alaskan_native_servicing_institution', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(alaskan_native_servicing_institution)'},
    {"canonical": 'native_hawaiian_servicing_institution', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(native_hawaiian_servicing_institution)'},
    {"canonical": 'school_of_forestry', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(school_of_forestry)'},
    {"canonical": 'veterinary_college', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(veterinary_college)'},
    {"canonical": 'dot_certified_disadvantage', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(dot_certified_disadvantage)'},
    {"canonical": 'self_certified_small_disadvantaged_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(self_certified_small_disadvantaged_business)'},
    {"canonical": 'small_disadvantaged_business', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(small_disadvantaged_business)'},
    {"canonical": 'c8a_program_participant', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(c8a_program_participant)'},
    {"canonical": 'historically_underutilized_business_zone_hubzone_firm', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(historically_underutilized_business_zone_hubzone_firm)'},
    {"canonical": 'sba_certified_8a_joint_venture', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(sba_certified_8a_joint_venture)'},
    {"canonical": 'usaspending_permalink', "duck_type": 'VARCHAR', "group": 'enrich', "bulk_expr": None, "feed_expr": 's(usaspending_permalink)'},
    # -- enrich: parent --
    {"canonical": 'direct_idv_count', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'direct_contract_count', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'direct_total_obligation', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'direct_base_and_all_options_value', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'direct_base_exercised_options_val', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'rollup_idv_count', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'rollup_contract_count', "duck_type": 'BIGINT', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'rollup_total_obligation', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'rollup_base_and_all_options_value', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    {"canonical": 'rollup_base_exercised_options_val', "duck_type": 'DOUBLE', "group": 'enrich', "bulk_expr": None, "feed_expr": None, "src": 'parent'},
    # ---- group: prov ----
    {"canonical": 'canonical_source', "duck_type": 'VARCHAR', "group": 'prov', "bulk_expr": None, "feed_expr": None},
    {"canonical": 'built_at', "duck_type": 'TIMESTAMP', "group": 'prov', "bulk_expr": None, "feed_expr": None},
]

_MACROS = "CREATE MACRO s(x) AS nullif(nullif(trim(x), ''), '-NONE-');\n"

# Index plan (locked by the Phase-3 column-selection pass) — presence-filtered at index() time.
BTREE_COLS = [
    "generated_unique_award_id", "contract_award_unique_key", "recipient_uei", "recipient_hash",
    "last_modified_date", "action_date", "period_of_performance_current_end_date",
    "total_obligation", "award_id_piid", "parent_award_id_piid", "naics_code",
    "product_or_service_code",
]
# NOTE: the award-type dimension index is award_type_code (BULK raw `type` crosswalks to canonical
# award_type_code; there is NO canonical output column named `type`). A bare "type" here was dead
# config — silently dropped by the presence-filter, never built — and is removed.
BITMAP_COLS = [
    "type_of_set_aside_code", "extent_competed", "awarding_agency_code",
    "awarding_sub_agency_code", "funding_agency_code", "recipient_state_code",
    "primary_place_of_performance_state_code", "award_type_code", "parent_award_type_code",
    "multiple_or_single_award_idv_code", "canonical_source",
]


# ---- COLUMN_SPEC derived helpers (all generated; nothing hand-transcribed) ---- #
def _canon_order() -> list[str]:
    return [c["canonical"] for c in COLUMN_SPEC]


def _cols(group: str) -> list[dict]:
    return [c for c in COLUMN_SPEC if c["group"] == group]


def _typed_null(c: dict) -> str:
    return f"CAST(NULL AS {c['duck_type']})"


_PARSE_SKIP = {"s", "TRY_CAST", "COALESCE", "CAST", "AS", "DOUBLE", "BIGINT", "BOOLEAN", "DATE",
               "TIMESTAMP", "VARCHAR", "INTEGER", "replace", "nullif", "trim", "NULL",
               "upper", "lower", "substr", "concat", "concat_ws",
               "CASE", "WHEN", "THEN", "END", "BETWEEN", "AND", "abs", "CURRENT_DATE"}

# The 10 parent_award-dataset net-new rollup aggregates + the join key. Materialized (semi-join
# scoped) as parent_latest; filled via the stage-2 enrich LEFT JOIN. parent_award_id FK NOT carried.
_PARENT_ROLLUP_COLS = [
    "direct_idv_count", "direct_contract_count", "direct_total_obligation",
    "direct_base_and_all_options_value", "direct_base_exercised_options_val",
    "rollup_idv_count", "rollup_contract_count", "rollup_total_obligation",
    "rollup_base_and_all_options_value", "rollup_base_exercised_options_val",
]


def _source_cols(kind: str) -> list[str]:
    """Distinct raw source column names referenced by the projection exprs — the scanner column list.
    kind: 'bulk' | 'feed' | 'parent'. 'parent' = the 10 net-new rollup cols + the join key
    generated_unique_award_id (they carry no expr in COLUMN_SPEC; enumerated explicitly). Double-quoted
    identifiers (source cols with non-identifier chars, e.g. the ``COVID-19`` supplemental amounts and
    the leading-digit ``1862_land_grant_college``) are captured EXACTLY; bare identifiers are parsed by
    stripping macro/cast wrappers. Phantom fragments are dropped by the presence-filter in build()."""
    if kind == "parent":
        return sorted(set(_PARENT_ROLLUP_COLS) | {PK_COL})
    import re
    key = "bulk_expr" if kind == "bulk" else "feed_expr"
    raw: set[str] = set()
    for c in COLUMN_SPEC:
        expr = c[key]
        if not expr:
            continue
        for q in re.findall(r'"([^"]+)"', expr):   # quoted source idents — exact (non-identifier chars)
            raw.add(q)
        bare = re.sub(r'"[^"]+"', " ", expr)        # strip quoted parts before the bare-token scan
        for tok in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", bare):
            if tok not in _PARSE_SKIP:
                raw.add(tok)
    return sorted(raw)


# =========================================================================================== #
# Generated SQL builders (pure strings — NO R2; safe to print for inspection)
# =========================================================================================== #
def _proj_select(side: str, built_at_iso: str, include_fresh: bool = True) -> str:
    """One per-source projection SELECT body in the canonical column order. side: 'bulk' | 'feed'.
    The include_fresh gate lives HERE (feed leg only): when include_fresh=False every feed_expr → a
    typed-NULL placeholder so _fresh_collapse_empty projects a schema-identical (but empty) fresh_latest.
    canonical_source is a typed-NULL placeholder here (derived per-key downstream); built_at is the
    injected literal. src="parent" rows have both exprs None → typed-NULL on both legs (filled via the
    parent enrich LEFT JOIN, never via a projection)."""
    lines = []
    for c in COLUMN_SPEC:
        canon = c["canonical"]
        if c["group"] == "prov":
            if canon == "canonical_source":
                lines.append(f"  {_typed_null(c)} AS canonical_source")
            else:  # built_at
                lines.append(f"  TIMESTAMP '{built_at_iso}' AS built_at")
            continue
        if side == "feed":
            expr = c["feed_expr"] if (include_fresh and c["feed_expr"] is not None) else _typed_null(c)
        else:  # bulk
            expr = c["bulk_expr"] if c["bulk_expr"] is not None else _typed_null(c)
        lines.append(f"  {expr} AS {canon}")
    return ",\n".join(lines)


def _enrich_replace_block() -> str:
    """The enrichment REPLACE block for `resolved`: overwrite every enrichment column keyed on the gua,
    INDEPENDENT of which source won the core. Each enrich col is SINGLE-SOURCE, routed to ONE of three
    LEFT-JOINed gua-unique collapses:
      • src == "parent"       → p.<col> from parent_latest (10 IDV rollup aggregates)
      • bulk_expr is not None  → b.<col> from bulk_latest   (BULK-only enrich)
      • else (feed-only)       → f.<col> from fresh_latest  (FRESH-only enrich)
    No COALESCE (no dual-source enrich exists). b/f/p are the LEFT-JOINed PK-unique collapses."""
    parts = []
    for c in _cols("enrich"):
        col = c["canonical"]
        if c.get("src") == "parent":
            leg = "p"
        elif c["bulk_expr"] is not None:
            leg = "b"
        else:
            leg = "f"
        parts.append(f"    {leg}.{col} AS {col}")
    return ",\n".join(parts)


def _bulk_collapse(built_at_iso: str) -> str:
    """BULK collapse → latest-per-gua, projection INLINED (ONE ~30.4M scan; bulk_proj never
    materialized). NOTE: BULK is already 1:1 on gua in contract scope (live: 30,419,755 rows =
    distinct gua = 0 null under the is_fpds probe) → this window is DEFENSIVE-ONLY; rn>1 never fires
    and dedup from the BULK leg is expected ≈ 0 (do NOT flag near-zero BULK dedup as a bug — P1-8).
    award_id is the stable BIGINT surrogate (award_search PK, non-null/unique in scope)."""
    bulk_proj = _proj_select("bulk", built_at_iso)
    return f"""-- ===== BULK collapse (inlined; ONE ~30.4M scan; DEFENSIVE-ONLY window — P1-8) ===== --
CREATE TEMP TABLE bulk_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     (recipient_uei IS NULL) ASC,
                     award_id DESC NULLS LAST) AS rn
  FROM (
    SELECT
{bulk_proj}
    FROM bulk_r
  )
  WHERE generated_unique_award_id IS NOT NULL
) WHERE rn = 1;
"""


def _fresh_collapse(built_at_iso: str) -> str:
    """FRESH collapse → latest-per-gua, projection INLINED. The download/awards re-pull duplicates keys
    across windows (publish lag) → this collapse deterministically keeps the latest report per gua.

    TIEBREAK NOTE (deliberate deviation from plan STEP 4's `award_latest_action_date DESC`): the window
    sits OUTSIDE the inlined projection, so its second-order tiebreak references the projected canonical
    `action_date` (the sentinel-CLAMPED alias of raw award_latest_action_date; out-of-range dates → NULL
    → NULLS LAST here). Ordering by the raw column instead would require carrying it through the inner
    SELECT and EXCLUDE-ing it — breaking the SELECT * EXCLUDE(rn) byte-identity with bulk_latest that
    _assert_collapse_schema_identity guards. This tiebreak only fires when last_modified_date ties across
    a single gua's FRESH re-pulls (a near-impossible collision; both candidates carry the same clamped
    value anyway), and generated_unique_award_id DESC is the deterministic final tiebreak → NO realistic
    winner change. The clamped `action_date` alias is the INTENTIONAL, schema-safe tiebreak."""
    fresh_proj = _proj_select("feed", built_at_iso, include_fresh=True)
    return f"""-- ===== FRESH collapse (inlined; download re-pulls duplicate keys → keep latest) ===== --
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     action_date DESC NULLS LAST,        -- clamped-alias tiebreak (intentional; see docstring)
                     generated_unique_award_id DESC NULLS LAST) AS rn
  FROM (
    SELECT
{fresh_proj}
    FROM fresh_r
  )
  WHERE generated_unique_award_id IS NOT NULL
) WHERE rn = 1;
"""


def _fresh_collapse_empty(built_at_iso: str) -> str:
    """include_fresh=FALSE path — data-driven emptiness, NOT a second SQL shape. Projects the SAME
    canonical column list/order/type (every feed_expr → CAST(NULL AS <duck_type>) via
    _proj_select(include_fresh=False)) with WHERE 1=0 → fresh_latest is EMPTY but schema-identical to
    the include_fresh=TRUE fresh_latest. fresh_r is NOT scanned (the projection is over `range(0)`, so
    no source table is referenced). core_union degenerates to BULK-only; every award resolves
    canonical_source='bulk'. The Arrow/Lance output schema is byte-identical either way."""
    fresh_proj = _proj_select("feed", built_at_iso, include_fresh=False)
    return f"""-- ===== FRESH collapse (EMPTY — include_fresh=FALSE; schema-identical, WHERE 1=0) ===== --
-- fresh_r is NOT opened this run; the projection is over a 0-row generator so the column list/order/
-- type match the include_fresh=TRUE fresh_latest EXACTLY (proven by smoke_fn's cross-toggle DESCRIBE).
CREATE TEMP TABLE fresh_latest AS
SELECT * EXCLUDE (rn) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     action_date DESC NULLS LAST,
                     generated_unique_award_id DESC NULLS LAST) AS rn
  FROM (
    SELECT
{fresh_proj}
    FROM range(0)
  )
  WHERE 1=0
) WHERE rn = 1;
"""


def _parent_leg(present: set[str] | None = None) -> str:
    """parent_award leg — SEMI-JOIN SCOPED to the contract spine (P0-1). The full ~987,705-row
    parent_award is materialized into parent_r (small: 11 narrow cols); this SQL semi-join then prunes
    it to the ~32,341 in-scope rows (96.7% is grant/assistance IDV rollups that never join → the
    WHERE gua IN (SELECT gua FROM bulk_latest) drops the dead 96.7% at query time, NOT at scan time).
    parent_award is already 1:1 on gua (live) → no collapse.

    Each of the 10 net-new rollup cols is CAST to its spec duck_type (the ONLY leg that would otherwise
    project bare parent_award-native types — closes the type-drift hole where a parent-schema change to
    string would silently emit VARCHAR under the 393-col contract). PRESENCE-AWARE: `present` is the
    resolved parent scanner column set; any rollup col absent from the live parent_award schema is
    emitted as CAST(NULL AS <duck_type>) instead of a bare ref that would DuckDB-binder-FAIL the whole
    ~30.7M build after the BULK scan already ran (the parent rollup names carry no bulk_expr/feed_expr,
    so they are the one spec leg never DEC-expr-compiled — degrade gracefully, never hard-bind). gua is
    the join key (NO parent_award_id — P0-2); NOT a collapse core; stage-2 enrich LEFT JOIN only."""
    parent_cols = [c for c in _cols("enrich") if c.get("src") == "parent"]
    # None ⇒ assume every rollup col present (the print/smoke path stubs all parent cols present); an
    # explicit set (from build()'s resolved parent_scan) drives the absent-col → typed-NULL degradation.
    present = present if present is not None else {c["canonical"] for c in parent_cols}
    proj_lines = []
    for c in parent_cols:
        col = c["canonical"]
        if col in present:
            proj_lines.append(f"  CAST({col} AS {c['duck_type']}) AS {col}")
        else:
            proj_lines.append(f"  CAST(NULL AS {c['duck_type']}) AS {col}")  # absent from live schema
    proj_lines.append(f"  {PK_COL} AS {PK_COL}")
    proj = ",\n".join(proj_lines)
    return f"""-- ===== parent_award leg — SEMI-JOIN SCOPED to bulk_latest (~32,341 rows; P0-1) ===== --
CREATE TEMP TABLE parent_latest AS
SELECT
{proj}
FROM parent_r
WHERE {PK_COL} IS NOT NULL
  AND {PK_COL} IN (SELECT {PK_COL} FROM bulk_latest);
"""


_M_ROWS_IN_BULK = "CREATE TEMP TABLE m_rows_in_bulk   AS SELECT count(*) AS c FROM bulk_latest;"
_M_ROWS_IN_FRESH = "CREATE TEMP TABLE m_rows_in_fresh  AS SELECT count(*) AS c FROM fresh_latest;"
_M_ROWS_IN_PARENT = "CREATE TEMP TABLE m_rows_in_parent AS SELECT count(*) AS c FROM parent_latest;"
_M_FRESH_ONLY_TAIL = (
    "CREATE TEMP TABLE m_fresh_only_tail AS\n"
    "  SELECT count(*) AS c FROM fresh_latest f ANTI JOIN bulk_latest b\n"
    "    ON f.generated_unique_award_id = b.generated_unique_award_id;"
)


def _stage1_sql(built_at_iso: str, include_fresh: bool = True,
                parent_present: set[str] | None = None) -> str:
    """STAGE 1 — macros, the BULK collapse (inlined; ONE ~30.4M scan), the FRESH collapse (populated OR
    empty-but-schema-identical per include_fresh), the semi-join-scoped parent leg, and the 1-row m_*
    metric captures. The two collapses (bulk_latest / fresh_latest) are asserted byte-identical by
    _assert_collapse_schema_identity BEFORE the union; parent_latest is EXCLUDED from that gate (it is a
    dimensional enrich, not a union member). Executed as ONE multi-statement script.

    parent_present is the resolved parent scanner column set (parent rollup canonical names == raw
    names — they carry no expr). Threaded into _parent_leg so a rollup col absent from the live
    parent_award schema degrades to CAST(NULL AS T) instead of hard-binder-failing the giant. Defaults
    to None → all-present (the smoke/print path stubs every parent col present)."""
    parts = [_MACROS, _bulk_collapse(built_at_iso)]
    parts.append(_fresh_collapse(built_at_iso) if include_fresh
                 else _fresh_collapse_empty(built_at_iso))
    parts.append(_parent_leg(parent_present))
    parts += [_M_ROWS_IN_BULK, _M_ROWS_IN_FRESH, _M_ROWS_IN_PARENT, _M_FRESH_ONLY_TAIL]
    return "\n".join(parts)


def _stage2_sql() -> str:
    """STAGE 2 — the merge: 2-source (BULK + FRESH) core resolution + 3-leg dimensional enrich, ONE
    physical artifact, free-as-you-go DROPs. Pipeline:
      (stage-1: bulk_latest / fresh_latest [≤1 row per gua each] + parent_latest [scope-filtered])
        → core_union   (UNION ALL BY NAME of the two collapsed CORES, tagged src + source_rank FRESH<BULK)
        → core_winner  (SINGLE 2-way window: argmax(last_modified_date) per gua, source_rank tiebreak →
                        tie=FRESH; PK-uniqueness structural — one survivor per gua)
        → resolved     (LEFT JOIN bulk_latest [b] + fresh_latest [f] + parent_latest [p] → 3-leg
                        single-source enrich REPLACE + w.src AS canonical_source)
        → canonical_out(locked canonical projection). NO tombstone leg (award_search has no
                        correction_delete_ind).
    All three enrich JOINs are to gua-unique collapses → no fan-out. NEVER clamp last_modified_date."""
    enrich_block = _enrich_replace_block()
    canon_cols = ", ".join(_canon_order())
    return f"""-- ===== two collapsed cores → vertical union, tagged src + source_rank (FRESH<BULK) ===== --
CREATE TEMP TABLE core_union AS
SELECT CAST('fresh' AS VARCHAR) AS src, CAST(1 AS INTEGER) AS source_rank, f.* FROM fresh_latest f
UNION ALL BY NAME
SELECT CAST('bulk'  AS VARCHAR) AS src, CAST(2 AS INTEGER) AS source_rank, b.* FROM bulk_latest b;

-- fresh_latest LIVES ON (enrichment source at the 3-leg JOIN below) — do NOT drop yet.

-- ===== SINGLE 2-way per-gua core resolution: argmax(last_modified_date), tie→FRESH ===== --
-- After the two collapses there is at most one row per source per gua → source_rank alone
-- disambiguates every cross-source tie; generated_unique_award_id DESC is defense-in-depth. Exactly
-- one survivor per gua (row_number()=1) → PK-uniqueness structural.
CREATE TEMP TABLE core_winner AS
SELECT * EXCLUDE (rn, source_rank) FROM (
  SELECT *, row_number() OVER (
            PARTITION BY generated_unique_award_id
            ORDER BY last_modified_date DESC NULLS LAST,
                     source_rank ASC,
                     generated_unique_award_id DESC) AS rn
  FROM core_union
) WHERE rn = 1;

-- core_winner supersedes core_union. Capture the merged-count metric first, then free-as-you-go DROP.
CREATE TEMP TABLE m_merged AS SELECT count(*) AS c FROM core_winner;
DROP TABLE core_union;

-- ===== 3-leg enrich fill: BULK-only (b), FRESH-only (f), parent_award rollups (p) ===== --
-- All three LEFT JOINs are to gua-unique per-gua collapses → no fan-out. canonical_source is derived
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
LEFT JOIN bulk_latest   b ON w.generated_unique_award_id = b.generated_unique_award_id
LEFT JOIN fresh_latest  f ON w.generated_unique_award_id = f.generated_unique_award_id
LEFT JOIN parent_latest p ON w.generated_unique_award_id = p.generated_unique_award_id;

-- resolved supersedes core_winner + the three enrich legs.
DROP TABLE core_winner;
DROP TABLE bulk_latest;
DROP TABLE fresh_latest;
DROP TABLE parent_latest;

-- ===== locked canonical projection → canonical_out (NO tombstone) ===== --
CREATE TEMP TABLE canonical_out AS
SELECT {canon_cols} FROM resolved;

DROP TABLE resolved;
"""


def _build_merge_sql(*, built_at_iso: str, include_fresh: bool = True, since: str | None = None) -> str:
    """FULL merge SQL (stage 1 + stage 2) concatenated — for print_merge_sql inspection ONLY. build()
    runs the two stages separately so the schema-identity gate runs between them (against the two
    collapses). --since is pushed into the TWO data scanners (build side); noted here only as a marker."""
    since_note = (f"-- --since={since} pushed into the TWO data scanners (BULK + FRESH)\n"
                  if since else "")
    return (since_note + _stage1_sql(built_at_iso, include_fresh=include_fresh)
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
    DeleteObjects in batches of <=1000 keys (the API hard cap). Bypasses Lance's native R2 multipart
    writer, which fails once the data files widen with '400 InvalidPart: All non-trailing parts must
    have the same length' — the same reason the FPDS spine publishes via boto3, not a direct-R2 writer."""
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


def _build_indices_local(local_ds: str) -> list[str]:
    """Build the BTREE/BITMAP scalar indices against a LOCAL Lance path; return the columns actually
    indexed (schema-presence filtered). Opened with lance.dataset(local_ds) and NO storage_options → the
    local-FS writer (no multipart), the ONLY R2-safe way to write indices (the native R2 object-writer
    streams adaptive-sized parts R2 rejects: 400 InvalidPart). Shared by build() (indices written into
    local_ds BEFORE the single _publish_local_to_r2 → published atomically WITH the data). Idempotent:
    replace=True rebuilds cleanly; the TypeError fallback covers older lance. Raises on the first failing
    column so callers fail-closed BEFORE any R2 mutation."""
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
    """The two COLLAPSES (bulk_latest / fresh_latest — SELECT * EXCLUDE(rn) over projection-shaped
    inners, so they carry the projections' exact (name, type) sequences) MUST be byte-identical before
    any union. Raise on mismatch — a hard build failure rather than a silent transposition (paired with
    UNION ALL BY NAME downstream). parent_latest is EXCLUDED (it is a dimensional enrich, not a union
    member). This runs WITHIN one build; the cross-toggle guarantee is smoke_fn's DESCRIBE check."""
    sigs = {}
    for name in ("bulk_latest", "fresh_latest"):
        rows = con.execute(f"DESCRIBE {name}").fetchall()  # (column_name, column_type, ...)
        sigs[name] = [(r[0], r[1]) for r in rows]
    if sigs["bulk_latest"] != sigs["fresh_latest"]:
        diff = [(b, f) for b, f in zip(sigs["bulk_latest"], sigs["fresh_latest"]) if b != f]
        raise RuntimeError(f"collapse schema mismatch bulk_latest vs fresh_latest; "
                           f"first divergences: {diff[:5]}")


def _record_run(*, include_fresh, rows_in_bulk, rows_in_fresh, rows_in_parent_award,
                parent_award_matched, rows_out, rows_out_floor, rows_out_floor_ok, dedup_collapsed,
                fresh_only_tail, bulk_only_body, fresh_corrections_applied, null_key_dropped,
                max_last_modified_date, max_action_date, columns, write_mode, indices_built, status,
                error, started, completed) -> None:
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
                "INSERT INTO ops.usaspending_award_canonical_runs (feed, include_fresh, rows_in_bulk, "
                "rows_in_fresh, rows_in_parent_award, parent_award_matched, rows_out, rows_out_floor, "
                "rows_out_floor_ok, dedup_collapsed, fresh_only_tail, bulk_only_body, "
                "fresh_corrections_applied, null_key_dropped, max_last_modified_date, max_action_date, "
                "columns, write_mode, indices_built, status, error_message, started_at, completed_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (FEED, include_fresh, rows_in_bulk, rows_in_fresh, rows_in_parent_award,
                 parent_award_matched, rows_out, rows_out_floor, rows_out_floor_ok, dedup_collapsed,
                 fresh_only_tail, bulk_only_body, fresh_corrections_applied, null_key_dropped,
                 max_last_modified_date, max_action_date, columns, write_mode,
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


def build(since: str | None = None, target_uri: str = CANONICAL_URI,
          include_fresh: bool = True) -> dict:
    import lance

    started = dt.datetime.now(dt.timezone.utc)
    so = _r2_so()
    # ONE naive-UTC literal, injected into both projections (NOT now()).
    built_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    built_at_iso = built_at.strftime("%Y-%m-%d %H:%M:%S.%f")

    # Pre-initialize every metric so a mid-merge crash still writes a coherent status='error' row.
    status, error = "error", None
    rows_in_bulk = rows_in_fresh = rows_in_parent = parent_award_matched = 0
    rows_out = rows_out_floor = dedup_collapsed = 0
    fresh_only_tail = bulk_only_body = fresh_corrections = null_key_dropped = 0
    rows_out_floor_ok = False
    max_last_modified = None
    max_action = None
    metrics: dict = {}
    built_idx: list[str] = []
    con = None
    local_ds = os.path.join(SCRATCH, "award_canonical_lance")
    try:
        os.makedirs(SCRATCH, exist_ok=True)
        shutil.rmtree(local_ds, ignore_errors=True)

        # --since pushed into the DATA scanners (naive TIMESTAMP for BULK; lexical ISO for FRESH). The
        # CONT% contract scope is pushed into the BULK scanner filter, NEVER the SQL body.
        bulk_filter = CONTRACT_FILTER
        feed_filter = None
        if since:
            bulk_filter += f" AND last_modified_date >= TIMESTAMP '{since}'"
            feed_filter = f"last_modified_date >= '{since}'"

        bulk_ds = lance.dataset(BULK_URI, storage_options=so)
        parent_ds = lance.dataset(PARENT_URI, storage_options=so)
        bulk_present = set(bulk_ds.schema.names)
        parent_present = set(parent_ds.schema.names)
        bulk_scan = [c for c in _source_cols("bulk") if c in bulk_present]
        parent_scan = [c for c in _source_cols("parent") if c in parent_present]

        con = _duck()
        log(f"registering sources (since={since}, include_fresh={include_fresh}, "
            f"contract-only) → target {target_uri}")
        # BULK: single pass into the inlined per-gua collapse → .to_reader() (rows_in from m_rows_in_bulk).
        con.register("bulk_r", bulk_ds.scanner(columns=bulk_scan, filter=bulk_filter).to_reader())
        # parent: FULL ~988K raw .to_table() (small — 11 narrow cols, tens of MB; the scanner has NO
        # filter). The semi-join scope-reduction to ~32K happens in the stage-1 SQL (_parent_leg), NOT
        # at scan time. Re-scannable for the enrich LEFT JOIN + exact m_rows_in_parent metric.
        con.register("parent_r", parent_ds.scanner(columns=parent_scan).to_table())
        # FRESH: scanner OPENED only when include_fresh (small ~98K distinct → .to_table(), re-scannable).
        # When include_fresh=False the fresh leg is _fresh_collapse_empty over range(0) → fresh_r unused.
        if include_fresh:
            fresh_ds = lance.dataset(FRESH_URI, storage_options=so)
            fresh_present = set(fresh_ds.schema.names)
            fresh_scan = [c for c in _source_cols("feed") if c in fresh_present]
            con.register("fresh_r", fresh_ds.scanner(columns=fresh_scan, filter=feed_filter).to_table())

        # Stage 1: BULK collapse + FRESH collapse (populated OR empty) + parent leg + m_* captures, then
        # ENFORCE schema identity on the two collapses (parent excluded). Stage 2: the merge with
        # free-as-you-go DROPs.
        con.execute(_stage1_sql(built_at_iso, include_fresh=include_fresh,
                                parent_present=set(parent_scan)))
        _assert_collapse_schema_identity(con)
        con.execute(_stage2_sql())

        rows_in_bulk = con.execute("SELECT c FROM m_rows_in_bulk").fetchone()[0]
        rows_in_fresh = con.execute("SELECT c FROM m_rows_in_fresh").fetchone()[0]
        rows_in_parent = con.execute("SELECT c FROM m_rows_in_parent").fetchone()[0]
        fresh_only_tail = con.execute("SELECT c FROM m_fresh_only_tail").fetchone()[0]
        merged = con.execute("SELECT c FROM m_merged").fetchone()[0]

        rows_out, pk_distinct = con.execute(
            f"SELECT count(*), count(DISTINCT {PK_COL}) FROM canonical_out").fetchone()

        # (1) FAIL-CLOSED PK-uniqueness (single-column; structural row_number()=1) — raise BEFORE publish.
        if rows_out != pk_distinct:
            raise RuntimeError(
                f"PK gate FAILED: count(*)={rows_out:,} != distinct {PK_COL}={pk_distinct:,} "
                f"({rows_out - pk_distinct:,} dup gua). Aborting publish.")

        # (2) FAIL-CLOSED rows_out FLOOR (full-universe only; relative to live BULK-scope this run).
        bulk_scope_rows = rows_in_bulk
        if since is None:
            rows_out_floor = int(bulk_scope_rows * 0.90)
            rows_out_floor_ok = rows_out >= rows_out_floor
            if not rows_out_floor_ok:
                raise RuntimeError(
                    f"rows_out FLOOR FAILED: rows_out={rows_out:,} < floor={rows_out_floor:,} "
                    f"(0.90 * live bulk_scope={bulk_scope_rows:,}). Aborting publish.")
        else:
            rows_out_floor_ok = True  # --since samples are exempt from the full-universe floor.

        # (3) FAIL-CLOSED tail-entered gate — ON only. The flip MUST grow the table by the FRESH tail;
        # closes the hole where FRESH collapses to only its overlap and verify #7 still passes while all
        # the fresh_only_tail awards are silently lost.
        if since is None and include_fresh:
            if rows_out <= bulk_scope_rows:
                raise RuntimeError(
                    f"tail gate FAILED: include_fresh=TRUE but rows_out={rows_out:,} "
                    f"<= bulk_scope={bulk_scope_rows:,} (fresh_only_tail did not enter). Aborting publish.")

        # Ledger metrics. BULK leg dedup expected ≈ 0 (already 1:1 — P1-8).
        dedup_collapsed = int(rows_in_bulk + rows_in_fresh - merged)
        # fresh_corrections_applied: awards FRESH won that BULK also holds (landed download corrections).
        fresh_corrections = con.execute(
            "SELECT count(*) FROM canonical_out WHERE canonical_source='fresh'").fetchone()[0]
        # parent_award_matched: rows that actually received a parent enrich (P0-2 — a real rollup col
        # non-NULL, NOT a nonsensical FK self-join).
        parent_award_matched = con.execute(
            "SELECT count(*) FROM canonical_out WHERE rollup_total_obligation IS NOT NULL").fetchone()[0]
        # bulk_only_body: contract spine rows with NO fresh counterpart (canonical_source='bulk').
        bulk_only_body = con.execute(
            "SELECT count(*) FROM canonical_out WHERE canonical_source='bulk'").fetchone()[0]
        null_key_dropped = 0  # both collapses drop NULL gua via WHERE ... IS NOT NULL (single-pass BULK
        # reader cannot be re-counted; the m_rows_in_* are post-collapse, post-NULL-drop by construction).
        max_last_modified = con.execute(
            "SELECT max(last_modified_date) FROM canonical_out "
            "WHERE last_modified_date <= now()").fetchone()[0]
        max_action = con.execute(
            "SELECT max(action_date) FROM canonical_out WHERE action_date <= CURRENT_DATE").fetchone()[0]

        log(f"merged={merged:,} rows_out={rows_out:,} rows_out_floor={rows_out_floor:,} "
            f"fresh_only_tail={fresh_only_tail:,} fresh_corrections={fresh_corrections:,} "
            f"rows_in_parent={rows_in_parent:,} parent_matched={parent_award_matched:,} "
            f"max_last_modified={max_last_modified} max_action={max_action}")

        # ── stream the result to a LOCAL Lance dir; boto3-publish (NO direct-R2 write) ──
        reader = con.sql("SELECT * FROM canonical_out").to_arrow_reader(batch_size=200_000)
        log(f"writing Lance LOCALLY → {local_ds}")
        lance.write_dataset(reader, local_ds, mode="overwrite",
                            data_storage_version=DATA_STORAGE_VERSION,
                            max_rows_per_file=MAX_ROWS_PER_FILE,
                            max_bytes_per_file=MAX_BYTES_PER_FILE)
        con.close()
        con = None

        # ── reclaim DuckDB RSS + spill BEFORE the RAM-heavy index sort (fold-isolation) ──
        # glibc does not return freed arenas to the OS on its own → malloc_trim forces it, so residual
        # DuckDB RSS cannot collide with the LANCE_BYPASS_SPILLING BTREE sort and trigger an OOM-SIGKILL
        # (an out-of-band kill the except/finally below CANNOT catch → no ledger row). Dropping DUCK_TMP
        # frees reconcile spill so the local index write cannot ENOSPC the shared ephemeral disk.
        import ctypes
        import gc as _gc
        del reader
        _gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except OSError:
            pass  # non-glibc (e.g. local macOS dev); Modal's debian_slim image is glibc
        shutil.rmtree(DUCK_TMP, ignore_errors=True)

        # ── build indices on the LOCAL dataset BEFORE publish (atomic fold) ──
        # local-FS writer (no multipart, R2-safe); _publish_local_to_r2's os.walk uploads the resulting
        # _indices/ together with the data fragments in ONE publish. A failed index raises HERE, before
        # _s3() and the wipe-then-upload ever run ⇒ the R2 SoR is never touched (all-or-nothing).
        built_idx = _build_indices_local(local_ds)
        log(f"indices built LOCALLY: {built_idx}")

        s3 = _s3()
        log(f"publishing local Lance (data + indices) → {target_uri} (boto3 uniform-part)…")
        published = _publish_local_to_r2(s3, target_uri, local_ds)
        log(f"published {published} files → {target_uri}")
        committed = lance.dataset(target_uri, storage_options=so).count_rows()
        status = "success"
        log(f"DONE → {target_uri} committed={committed:,}")
        metrics = {"target_uri": target_uri, "since": since, "include_fresh": bool(include_fresh),
                   "rows_in_bulk": int(rows_in_bulk), "rows_in_fresh": int(rows_in_fresh),
                   "rows_in_parent_award": int(rows_in_parent),
                   "parent_award_matched": int(parent_award_matched), "rows_out": int(rows_out),
                   "rows_out_floor": int(rows_out_floor), "rows_out_floor_ok": bool(rows_out_floor_ok),
                   "dedup_collapsed": int(dedup_collapsed), "fresh_only_tail": int(fresh_only_tail),
                   "bulk_only_body": int(bulk_only_body),
                   "fresh_corrections_applied": int(fresh_corrections),
                   "null_key_dropped": int(null_key_dropped),
                   "max_last_modified_date": max_last_modified, "max_action_date": max_action,
                   "pk_unique": True, "columns": len(COLUMN_SPEC), "committed_rows": int(committed),
                   "files_published": int(published), "indices_built": built_idx,
                   "write_mode": "overwrite", "status": status}
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        log(f"FAILED: {error}")
        raise
    finally:
        if con is not None:
            con.close()
        _record_run(include_fresh=bool(include_fresh), rows_in_bulk=int(rows_in_bulk),
                    rows_in_fresh=int(rows_in_fresh), rows_in_parent_award=int(rows_in_parent),
                    parent_award_matched=int(parent_award_matched), rows_out=int(rows_out),
                    rows_out_floor=int(rows_out_floor), rows_out_floor_ok=bool(rows_out_floor_ok),
                    dedup_collapsed=int(dedup_collapsed), fresh_only_tail=int(fresh_only_tail),
                    bulk_only_body=int(bulk_only_body),
                    fresh_corrections_applied=int(fresh_corrections),
                    null_key_dropped=int(null_key_dropped),
                    max_last_modified_date=max_last_modified, max_action_date=max_action,
                    columns=len(COLUMN_SPEC), write_mode="overwrite", indices_built=built_idx,
                    status=status, error=error, started=started,
                    completed=dt.datetime.now(dt.timezone.utc))
        shutil.rmtree(SCRATCH, ignore_errors=True)
    return metrics


def index(target_uri: str = CANONICAL_URI) -> dict:
    """Build the BTREE/BITMAP scalar indices DIRECT-R2, in place — sample/dev ONLY. At the ~30M-row
    giant scale the direct-R2 write trips R2's uniform-part rule (400 InvalidPart) and the BTREE sort
    OOMs the bounded DataFusion pool; the giant path is the Modal wrapper's /tmp-staged append-only
    index_fn. This shipped index() is kept for small-slice sample builds. LANCE_BYPASS_SPILLING keeps
    the small sort in-RAM. Idempotent (replace=True with TypeError fallback)."""
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


def verify(target_uri: str = CANONICAL_URI, include_fresh: bool = True,
           rows_out_floor: int | None = None, bulk_scope_rows: int | None = None) -> dict:
    """Read-back proof (independent scanner → DuckDB; materializes — the giant verify path). GATES
    (each failures.append → verdict=fail):
      1. PK-unique: count(*) == count(DISTINCT generated_unique_award_id).
      2. rows_out floor: count(*) >= rows_out_floor (when passed / from the same run's ledger).
      3. cols present: len(ds.schema.names) == len(COLUMN_SPEC) (exact locked 393).
      4. index presence by SUBSTRING over ds.list_indices() (subaward-correct form, never exact match).
      5. last_modified frontier: max(last_modified_date) WHERE <= now() non-NULL.
      6. canonical_source domain ⊆ {fresh,bulk}.
      7. fresh-won ⇔ include_fresh: TRUE → count(fresh) > 0; FALSE → == 0.
      8. tail-grew ⇔ include_fresh: TRUE → count(*) > bulk_scope_rows; FALSE → == bulk_scope_rows
         (when bulk_scope_rows is known — #7 alone passes even if only the overlap survived).
      9. null_key: count(*) WHERE gua IS NULL == 0.
      10. built_at single literal: count(DISTINCT built_at) == 1.
      Reported (not gated): parent_award_matched / rows_in_parent_award coverage ratio."""
    import lance
    so = _r2_so()
    ds = lance.dataset(target_uri, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    idx_blob = " ".join(str(x) for x in idx)
    con = _duck()
    # register() creates a VIEW c_src; CREATE TEMP TABLE c materializes from it (the giant verify does a
    # full materialize — the checks multi-scan c, and a single-pass .to_reader() would exhaust).
    con.register("c_src", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE c AS SELECT * FROM c_src")

    total, pk_distinct = con.execute(
        f"SELECT count(*), count(DISTINCT {PK_COL}) FROM c").fetchone()
    mx_lastmod = con.execute(
        "SELECT max(last_modified_date) FROM c WHERE last_modified_date <= now()").fetchone()[0]
    mx_action = con.execute(
        "SELECT max(action_date) FROM c WHERE action_date <= CURRENT_DATE").fetchone()[0]
    src_dist = dict(con.execute(
        "SELECT canonical_source, count(*) FROM c GROUP BY 1 ORDER BY 2 DESC").fetchall())
    built_at_distinct = con.execute("SELECT count(DISTINCT built_at) FROM c").fetchone()[0]
    null_key = con.execute(f"SELECT count(*) FROM c WHERE {PK_COL} IS NULL").fetchone()[0]
    bad_source = con.execute(
        "SELECT count(*) FROM c WHERE canonical_source IS NULL "
        "OR canonical_source NOT IN ('fresh','bulk')").fetchone()[0]
    fresh_won = con.execute("SELECT count(*) FROM c WHERE canonical_source='fresh'").fetchone()[0]
    parent_matched = con.execute(
        "SELECT count(*) FROM c WHERE rollup_total_obligation IS NOT NULL").fetchone()[0]
    con.close()

    failures: list[str] = []
    if total != pk_distinct:
        failures.append(f"PK-unique: count(*)={total:,} != distinct {PK_COL}={pk_distinct:,}")
    if rows_out_floor is not None and total < rows_out_floor:
        failures.append(f"rows_out floor: count(*)={total:,} < rows_out_floor={rows_out_floor:,}")
    if len(ds.schema.names) != len(COLUMN_SPEC):
        failures.append(f"cols present: {len(ds.schema.names)} != len(COLUMN_SPEC)={len(COLUMN_SPEC)}")
    if PK_COL not in idx_blob:
        failures.append(f"index presence: '{PK_COL}' substring absent from list_indices() blob")
    if mx_lastmod is None:
        failures.append("last_modified frontier: max(last_modified_date <= now()) is NULL")
    if bad_source:
        failures.append(f"canonical_source domain: {bad_source:,} rows NULL or ∉ {{fresh,bulk}}")
    if include_fresh and fresh_won <= 0:
        failures.append("fresh-won ⇔ include_fresh: include_fresh=TRUE but canonical_source='fresh' == 0")
    if (not include_fresh) and fresh_won != 0:
        failures.append(f"fresh-won ⇔ include_fresh: include_fresh=FALSE but fresh count={fresh_won:,}")
    if bulk_scope_rows is not None:
        if include_fresh and total <= bulk_scope_rows:
            failures.append(f"tail-grew: include_fresh=TRUE but count(*)={total:,} <= "
                            f"bulk_scope={bulk_scope_rows:,}")
        if (not include_fresh) and total != bulk_scope_rows:
            failures.append(f"tail-grew: include_fresh=FALSE but count(*)={total:,} != "
                            f"bulk_scope={bulk_scope_rows:,}")
    if null_key:
        failures.append(f"null_key: {null_key:,} rows with NULL {PK_COL}")
    if built_at_distinct != 1:
        failures.append(f"built_at not a single literal: distinct={built_at_distinct}")

    return {"uri": target_uri, "rows_out": int(total),
            "pk_unique": bool(total == pk_distinct), "pk_dupes": int(total - pk_distinct),
            "null_key_rows": int(null_key),
            "max_last_modified_date": str(mx_lastmod) if mx_lastmod is not None else None,
            "max_action_date": str(mx_action) if mx_action is not None else None,
            "built_at_distinct": int(built_at_distinct),
            "canonical_source_distribution": {k: int(v) for k, v in src_dist.items()},
            "fresh_won_rows": int(fresh_won), "parent_award_matched": int(parent_matched),
            "columns": len(ds.schema.names), "indices": idx,
            "include_fresh": bool(include_fresh),
            "failures": failures, "pass": not failures}


def _post_callback(url: str | None, payload: dict, attempts: int = 3) -> None:
    """POST the flat terminal metadata to a Trigger.dev waitpoint callback URL (parity only — there is
    NO Trigger.dev schedule/cron for this canonical; rebuild is operator-initiated). No-op when url
    None (every manual/Modal run)."""
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
            target_uri: str = CANONICAL_URI, include_fresh: bool = True) -> dict:
    """Parity entrypoint — the full build → index → verify chain as ONE terminal unit, then POST the
    (optional) callback. NO Trigger.dev schedule is wired (NON-GOAL); this exists for chain parity with
    the sibling canonicals. On the giant path build() folds its own index; this refresh() is the
    small-slice convenience chain. Raises (→ callback status='error') on a failed verify."""
    status, payload = "error", {"feed": FEED, "dataset_uri": target_uri}
    try:
        b = build(since=since, target_uri=target_uri, include_fresh=include_fresh)
        idx = index(target_uri=target_uri)
        v = verify(target_uri=target_uri, include_fresh=include_fresh,
                   rows_out_floor=b.get("rows_out_floor") or None,
                   bulk_scope_rows=b.get("rows_in_bulk"))
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
# CLI
# =========================================================================================== #
def _arg_val(flag: str, argv: list[str], default: str | None = None) -> str | None:
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    return default


def _parse_include_fresh(v: str | None) -> bool:
    """Explicit truthy parse — NOT bool(v) (bool("false") is True, inverting intent). A bare/absent
    value defaults to True."""
    if v is None or v == "":
        return True
    return v.lower() in ("1", "true", "yes", "on")


def main():
    argv = sys.argv[1:]
    cmd = argv[0] if argv else "verify"
    target_uri = _arg_val("--target-uri", argv, CANONICAL_URI)
    include_fresh = _parse_include_fresh(_arg_val("--include-fresh", argv, None))
    if cmd == "init_ops":
        init_ops()
    elif cmd == "build":
        print(json.dumps(build(since=_arg_val("--since", argv, None), target_uri=target_uri,
                               include_fresh=include_fresh), indent=2, default=str))
    elif cmd == "index":
        print(json.dumps(index(target_uri=target_uri), indent=2, default=str))
    elif cmd == "verify":
        print(json.dumps(verify(target_uri=target_uri, include_fresh=include_fresh),
                         indent=2, default=str))
    elif cmd == "print_merge_sql":
        print(_build_merge_sql(built_at_iso="2026-07-04 00:00:00.000000",
                               include_fresh=include_fresh,
                               since=_arg_val("--since", argv, None)))
    else:
        print(f"unknown command: {cmd} (init_ops|build|index|verify|print_merge_sql)")
        sys.exit(2)


if __name__ == "__main__":
    main()
