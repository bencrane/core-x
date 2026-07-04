"""USAspending FPDS L2 satellites — prime-award CAPACITY/STATE + MOD-DELTA (LOCAL CLI).

Two derived, index-served access-path MVs built from ONE shared award-partitioned window pass over
the L1 spine (`usaspending_fpds_canonical_txn`, 108M×392). The spine is a denormalized ledger ("what
did the government record"); these L2 tables answer the origination engine's two hot questions the
spine cannot answer live:

  A) usaspending_fpds_prime_award_state  (award grain: contract_award_unique_key)
     — CAPACITY STARVATION. Per prime-award root: life-to-date obligated, current + potential ceiling,
       remaining headroom, consumed %, expiry horizon, IDV→child-order rollup. One row per award/IDV/order.
  B) usaspending_fpds_mod_delta          (mod grain: contract_transaction_unique_key)
     — KINETIC EVENTS. Per MODIFYING transaction: the row-to-row first-difference on the ~18 footprint-
       relevant columns (Δceiling, Δobligation, Δexpiry-days, identity boundary), classified by action_type.

SHARED BUILD (the efficiency play): the 108M-row external sort is paid ONCE. A ~37-col narrow projection
of the spine (VARCHAR→numeric/date TRY_CASTs done once) drives a single
`PARTITION BY contract_award_unique_key ORDER BY <5-key total order>` window consumed two ways —
ROW_NUMBER()=1 argmax terminal snapshot + a GROUP-BY additive aggregate (→ STATE) and LAG() first-diff
(→ DELTA). The only extra work is the IDV parent→child obligation rollup: a hash aggregate over the
already-collapsed ~40-55M award-grain staging (construct-and-validate parent key against the real
IDV-key set — grammar-self-checking, never a blind parse), NOT a second 108M scan.

TERNARY award_kind (locked): idv (idv_type_code present) | order (has parent IDV) | definitive. The
origination engine strictly separates a definitive recompete from a parent-IDV ceiling check.

DISCIPLINES (mirrors the spine; d.8 / fleet rules):
  • module-top os.environ.setdefault("LANCE_BYPASS_SPILLING","true") BEFORE any import lance.
  • NO direct-R2 write (Giants 400 InvalidPart) — LOCAL Lance write → boto3 uniform-part publish
    (reuses the spine's proven _publish_local_to_r2 / index-delta plumbing). v2.1, max_rows_per_file.
  • ONE naive-UTC built_at literal + ONE build_date literal injected per build (NOT now()).
  • the 5-key TOTAL ladder ORDER BY (…, contract_transaction_unique_key) is MANDATORY on both the STATE
    argmax and the DELTA LAG — same-(action_date,last_modified_date) correction rows exist; without it
    snapshots/deltas flip across rebuilds. DELTA is wall-clock-free (pure function of the spine).
  • fail-closed PK-uniqueness gate per table BEFORE publish. NO auto-retries; overwrite idempotency.
  • only federal_action_obligation is SUM-able; every *_value/total_* field is cumulative → ladder-final
    argmax snapshot, never SUM (summing multiplies value by mod count).

    # SAMPLE (bounded --since to a _sample target — never prod):
    doppler run -p core-x -c prd -- uv run --no-project \
      --with 'pylance>=7' --with 'pyarrow>=17' --with 'duckdb>=1.5,<2' \
      --with 'boto3>=1.35' --with 'psycopg[binary]>=3.2' \
      python3 -m pipelines.usaspending.usaspending_fpds_l2 build --since 2025-10-01 \
        --state-uri s3://data-sink/active/_sample/fpds_prime_award_state_sample/ \
        --delta-uri s3://data-sink/active/_sample/fpds_mod_delta_sample/

    # FULL (>=96 GiB box): FPDS_L2_DUCKDB_MEM=72GB FPDS_L2_DUCKDB_THREADS=8
    python3 -m pipelines.usaspending.usaspending_fpds_l2 build       # then: index --table {state|delta}; verify
    python3 -m pipelines.usaspending.usaspending_fpds_l2 init_ops
    python3 -m pipelines.usaspending.usaspending_fpds_l2 index  --table state [--state-uri URI]
    python3 -m pipelines.usaspending.usaspending_fpds_l2 verify --table delta [--delta-uri URI]
    python3 -m pipelines.usaspending.usaspending_fpds_l2 print_sql   # dump generated SQL (no R2)
"""
from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import sys

# In-RAM scalar-index sort — the small DataFusion external-merge pool OOMs on a >=40M-row BTREE build.
# Set BEFORE any lance call (fleet rule). Import-time, mirrors the spine.
os.environ.setdefault("LANCE_BYPASS_SPILLING", "true")

# Reuse the spine's proven R2 plumbing (LOCAL-write → boto3 uniform-part publish; append-only index
# delta; orphan-index GC). Co-located sibling in the same package; the Modal image ships the whole
# pipelines/ tree, so the import resolves in-container. Importing the spine module is side-effect-safe
# (no R2 at import; it also sets LANCE_BYPASS_SPILLING).
from pipelines.usaspending import usaspending_fpds_canonical as _spine

BUCKET = "data-sink"
ACTIVE = "s3://data-sink/active"

SPINE_URI = f"{ACTIVE}/usaspending_fpds_canonical_txn/"
STATE_URI = f"{ACTIVE}/usaspending_fpds_prime_award_state/"
DELTA_URI = f"{ACTIVE}/usaspending_fpds_mod_delta/"

DATA_STORAGE_VERSION = "2.1"
MAX_ROWS_PER_FILE = 1_048_576          # valid ONLY on the boto3-publish path (uniform parts)
MAX_BYTES_PER_FILE = 90 * 1024**3

SCRATCH = os.environ.get("FPDS_L2_SCRATCH", "/tmp/fpds_l2_stage")
DUCK_MEM = os.environ.get("FPDS_L2_DUCKDB_MEM", "8GB")
DUCK_TMP = os.environ.get("FPDS_L2_DUCKDB_TEMP_DIR", "/tmp/fpds_l2_duckdb")
DUCK_THREADS = int(os.environ.get("FPDS_L2_DUCKDB_THREADS", "4"))

STATE_FEED = "usaspending_fpds_prime_award_state"
DELTA_FEED = "usaspending_fpds_mod_delta"
OPS_SQL_FILE = "ops_usaspending_fpds_l2_runs.sql"
STATE_OPS_TABLE = "ops.usaspending_fpds_prime_award_state_runs"
DELTA_OPS_TABLE = "ops.usaspending_fpds_mod_delta_runs"

RECON_TOLERANCE = 1.0  # $ — |life_to_date − total_dollars_obligated_snapshot| above this = a recon fail

# ── §4 index plan — per table, presence-filtered at build/index time ─────────────────────
STATE_BTREE = ["contract_award_unique_key", "recipient_uei", "recipient_hash",
               "parent_award_key_resolved", "solicitation_identifier", "award_id_piid",
               "current_end_date", "days_to_expiry", "consumed_pct", "remaining_ceiling_headroom"]
STATE_BITMAP = ["award_kind", "idv_type_code", "awarding_agency_code", "type_of_set_aside_code",
                "award_type_code", "terminal_action_type_code", "is_terminated",
                "is_expired_no_followon", "parent_match_flag", "potential_ceiling_is_fallback",
                "has_child_idv"]
DELTA_BTREE = ["contract_transaction_unique_key", "contract_award_unique_key", "action_date",
               "recipient_uei", "delta_potential_ceiling", "delta_federal_action_obligation"]
DELTA_BITMAP = ["action_type_code", "award_kind", "action_type_klass", "is_scope_increase",
                "is_termination_event", "identity_changed", "awarding_agency_code", "award_pool"]

# ── spine source columns the shared pass scans (all VERIFIED present on the 392-col spine) ─
SPINE_SCAN_COLS = [
    # keys + ladder order
    "contract_award_unique_key", "contract_transaction_unique_key",
    "action_date", "last_modified_date", "modification_number", "transaction_number",
    "action_type_code", "idv_type_code",
    # money (federal_action_obligation is the ONLY summable measure; the rest are cumulative snapshots)
    "federal_action_obligation", "current_total_value_of_award", "base_and_exercised_options_value",
    "base_and_all_options_value", "total_dollars_obligated",
    "potential_total_value_awar",           # VARCHAR landmine — TRY_CAST → DOUBLE in projection
    # time
    "period_of_performance_current_end_date", "period_of_performance_start_date",
    "period_of_perf_potential_e",           # VARCHAR landmine — TRY_CAST → DATE in projection
    "ordering_period_end_date",
    # hierarchy
    "award_id_piid", "parent_award_id_piid", "parent_award_agency_id", "awarding_sub_agency_code",
    # identity (the footprint identity set: novation/re-rep/transfer boundary detection)
    "recipient_uei", "recipient_hash", "recipient_name", "cage_code", "business_categories",
    "contracting_officers_determination_of_business_size", "funding_office_code", "parent_uei",
    "recipient_county_name",
    # facets
    "awarding_agency_code", "naics_code", "product_or_service_code",
    "type_of_set_aside_code", "award_type_code", "solicitation_identifier", "canonical_source",
]

# action_type → coarse operational class (grounded in the mod-footprint recon + fpds_action_type_ref;
# ADVISORY — the actual delta sign, never AF-slice membership, drives is_scope_increase). 'Y' is the one
# diagnosed non-standard code (outside the FPDS 20-code set; small-$ admin + minor re-representations) →
# its own 'nonstandard' klass; a truly-unseen future code falls to 'unclassified'. Deltas are computed
# for ALL codes regardless, so nothing is silently dropped.
_KLASS_CASE = """CASE
    WHEN action_type_code = 'G' THEN 'option_exercise'
    WHEN action_type_code IN ('A','B','D','H','L') THEN 'scope_change'
    WHEN action_type_code = 'C' THEN 'funding_only'
    WHEN action_type_code IN ('E','F','X','N') THEN 'termination'
    WHEN action_type_code IN ('J','P','R','T','V','W') THEN 'identity_boundary'
    WHEN action_type_code IN ('K','M','S') THEN 'admin'
    WHEN action_type_code = 'Y' THEN 'nonstandard'
    ELSE 'unclassified' END"""



def log(m):
    print(f"[{dt.datetime.now(dt.timezone.utc).isoformat()}] {m}", flush=True)


def _duck():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute(f"PRAGMA threads={DUCK_THREADS}")
    os.makedirs(DUCK_TMP, exist_ok=True)
    con.execute(f"SET memory_limit='{DUCK_MEM}'")
    con.execute(f"SET temp_directory='{DUCK_TMP}'")
    con.execute("SET preserve_insertion_order=false")
    return con


# ════════════════════════════════════════════════════════════════════════════════════════
# SQL — one narrow projection + one window spec feed BOTH tables
# ════════════════════════════════════════════════════════════════════════════════════════
def _projection_sql() -> str:
    """spine_src (registered scanner reader) → spine_proj TEMP TABLE. Single pass drains the R2 reader
    into a re-scannable local table; the two VARCHAR→numeric/date casts happen ONCE here so both the
    STATE and DELTA consumers see typed values. award_kind (ternary) is derived here — award-invariant,
    so the terminal-row value is authoritative for STATE and the per-row value is used by DELTA."""
    return """
CREATE TEMP TABLE spine_proj AS
SELECT
    contract_award_unique_key                                             AS cauk,
    contract_transaction_unique_key                                       AS ctuk,
    action_date, last_modified_date, modification_number, transaction_number,
    action_type_code, idv_type_code,
    CASE WHEN idv_type_code IS NOT NULL AND idv_type_code <> '' THEN 'idv'
         WHEN parent_award_id_piid IS NOT NULL AND parent_award_id_piid <> '' THEN 'order'
         ELSE 'definitive' END                                            AS award_kind,
    federal_action_obligation,
    current_total_value_of_award, base_and_exercised_options_value,
    base_and_all_options_value, total_dollars_obligated,
    TRY_CAST(potential_total_value_awar AS DOUBLE)                        AS pot_val,
    period_of_performance_current_end_date                               AS pop_cur_end,
    period_of_performance_start_date                                     AS pop_start,
    TRY_CAST(period_of_perf_potential_e AS DATE)                          AS pot_end,
    ordering_period_end_date                                            AS ord_end,
    award_id_piid, parent_award_id_piid, parent_award_agency_id, awarding_sub_agency_code,
    recipient_uei, recipient_hash, recipient_name, cage_code, business_categories,
    contracting_officers_determination_of_business_size                 AS co_det,
    funding_office_code, parent_uei, recipient_county_name,
    awarding_agency_code, naics_code, product_or_service_code,
    type_of_set_aside_code, award_type_code, solicitation_identifier, canonical_source
FROM spine_src;
"""


def _stage_sql(build_date_iso: str, built_at_iso: str) -> str:
    """win (single 5-key-ordered window: LAG for DELTA, ROW_NUMBER desc for STATE terminal snapshot)
    + agg (additive SUM/COUNT/MIN/MAX/BOOL_OR) → state_final (capacity math + IDV rollup) + delta_out
    (footprint-pruned first-difference). All derived from the ONE spine_proj sort."""
    return f"""
-- one window spec, three orderings, over the single narrow projection ─────────────────────
CREATE TEMP TABLE win AS
SELECT *,
    ROW_NUMBER()                          OVER lad_desc AS rn_final,
    LAG(current_total_value_of_award)     OVER lad AS p_ctv,
    LAG(base_and_exercised_options_value) OVER lad AS p_beo,
    LAG(base_and_all_options_value)       OVER lad AS p_bao,
    LAG(total_dollars_obligated)          OVER lad AS p_tdo,
    LAG(pot_val)                          OVER lad AS p_pot,
    LAG(pop_cur_end)                      OVER lad AS p_pce,
    LAG(pot_end)                          OVER lad AS p_pend,
    LAG(ord_end)                          OVER lad AS p_ord,
    LAG(pop_start)                        OVER lad AS p_pstart,
    LAG(recipient_uei)                    OVER lad AS p_uei,
    LAG(recipient_hash)                   OVER lad AS p_hash,
    LAG(recipient_name)                   OVER lad AS p_name,
    LAG(cage_code)                        OVER lad AS p_cage,
    LAG(business_categories)              OVER lad AS p_bizcat,
    LAG(co_det)                           OVER lad AS p_codet,
    LAG(funding_office_code)              OVER lad AS p_foc,
    LAG(parent_uei)                       OVER lad AS p_puei,
    LAG(recipient_county_name)            OVER lad AS p_county,
    LAG(ctuk)                             OVER lad AS p_ctuk,
    LAG(action_date)                      OVER lad AS p_action_date
FROM spine_proj
WINDOW
    lad AS (PARTITION BY cauk
            ORDER BY action_date, last_modified_date, modification_number,
                     transaction_number, ctuk),
    lad_desc AS (PARTITION BY cauk
            ORDER BY action_date DESC, last_modified_date DESC, modification_number DESC,
                     transaction_number DESC, ctuk DESC);

-- additive award-grain aggregates (federal_action_obligation is the ONLY summable measure) ─
CREATE TEMP TABLE agg AS
SELECT cauk,
    SUM(federal_action_obligation)              AS life_to_date_obligated,
    COUNT(*)                                    AS ladder_txn_count,
    MIN(action_date)                            AS first_action_date,
    MAX(action_date)                            AS last_action_date,
    BOOL_OR(action_type_code IN ('E','F','X'))  AS is_terminated
FROM spine_proj GROUP BY cauk;

-- STATE staging: terminal (argmax) snapshot ⋈ additive aggregates. Ceiling/time selection branches
-- on award_kind. current_total_value_of_award is obligated-to-date (a reconciliation twin), NOT a
-- ceiling — the current authorization is base_and_exercised_options_value; the max ceiling is the
-- potential value (TRY_CAST, COALESCE→base_and_all_options on the FRESH-winner typed-NULL case).
CREATE TEMP TABLE state_stage AS
SELECT
    w.cauk                                       AS contract_award_unique_key,
    w.award_kind,
    w.idv_type_code, w.award_id_piid,
    w.awarding_agency_code, w.awarding_sub_agency_code,
    w.recipient_uei, w.recipient_hash, w.recipient_name,
    w.naics_code, w.product_or_service_code, w.type_of_set_aside_code, w.award_type_code,
    w.solicitation_identifier,
    a.life_to_date_obligated,
    CASE WHEN w.award_kind = 'idv' THEN NULL
         ELSE w.base_and_exercised_options_value END                     AS current_authorized_ceiling,
    CASE WHEN w.award_kind = 'idv' THEN NULL
         ELSE w.current_total_value_of_award END                         AS current_total_value_of_award,
    CASE WHEN w.award_kind = 'idv' THEN w.base_and_all_options_value
         ELSE COALESCE(w.pot_val, w.base_and_all_options_value) END       AS potential_ceiling,
    CASE WHEN w.award_kind = 'idv' THEN NULL   -- N/A: IDV ceiling is base_and_all_options by design
         ELSE (w.pot_val IS NULL AND w.base_and_all_options_value IS NOT NULL) END AS potential_ceiling_is_fallback,
    w.total_dollars_obligated                    AS total_dollars_obligated_snapshot,
    (a.life_to_date_obligated - w.total_dollars_obligated)               AS obligation_reconciliation_delta,
    CASE WHEN w.award_kind = 'idv' THEN w.ord_end ELSE w.pop_cur_end END  AS current_end_date,
    CASE WHEN w.award_kind = 'idv' THEN NULL
         ELSE COALESCE(w.pot_end, w.pop_cur_end) END                      AS potential_end_date,
    w.pop_start                                  AS pop_start_date,
    a.ladder_txn_count, a.first_action_date, a.last_action_date, a.is_terminated,
    w.action_type_code                           AS terminal_action_type_code,
    w.parent_award_id_piid, w.parent_award_agency_id,
    w.canonical_source                           AS canonical_source_final
FROM win w JOIN agg a USING (cauk)
WHERE w.rn_final = 1;

-- parent resolution — construct-and-validate, keyed on the PRESENCE of a parent (piid), kind-agnostic
-- so a NESTED IDV (BPA/BOA under an FSS) resolves to its parent too, not just orders. Candidate parent
-- key = CONT_IDV_<parentPIID>_<parentAgency> concatenated VERBATIM (NO case-fold: the spine's
-- contract_award_unique_key is trim-only / case-PRESERVING — s() at spine build — and USAspending's
-- Broker concatenates the raw components, so a byte match REQUIRES verbatim; an upper() on one side
-- alone silently mis-flags every lower/mixed-case PIID as dangling → understated IDV consumption).
-- Grammar-self-checking: a wrong candidate simply fails the semi-join → dangling, NEVER a silent
-- mis-rollup. 'self' = no parent piid (definitive / standalone IDV). NULL agency → synth is NULL
-- (DuckDB `x||NULL`=NULL) → no match → dangling (can't resolve without the agency component).
CREATE TEMP TABLE idv_keys AS
SELECT contract_award_unique_key AS idv_key FROM state_stage WHERE award_kind = 'idv';

CREATE TEMP TABLE state_resolved AS
SELECT s.*,
    CASE WHEN s.parent_award_id_piid IS NOT NULL AND s.parent_award_agency_id IS NOT NULL
         THEN 'CONT_IDV_' || s.parent_award_id_piid || '_' || s.parent_award_agency_id
         END                                                             AS parent_award_key_synth,
    CASE WHEN s.parent_award_id_piid IS NULL THEN 'self'
         WHEN k.idv_key IS NOT NULL          THEN 'resolved'
         ELSE 'dangling' END                                            AS parent_match_flag,
    CASE WHEN s.parent_award_id_piid IS NULL THEN s.contract_award_unique_key
         WHEN k.idv_key IS NOT NULL          THEN k.idv_key
         ELSE NULL END                                                  AS parent_award_key_resolved
FROM state_stage s
LEFT JOIN idv_keys k
       ON k.idv_key = ('CONT_IDV_' || s.parent_award_id_piid || '_' || s.parent_award_agency_id);

-- IDV child rollup — the IDV capacity denominator = Σ life-to-date over the ENTIRE resolved SUBTREE
-- (all descendant orders, through any depth of nested IDVs), NOT the IDV header's own ~$0 line.
-- RECURSIVE transitive fold: each award's obligation is attributed to EVERY ancestor IDV in its chain,
-- so a GWAC/FSS sees the orders placed under BPAs beneath it, not merely its own direct orders. The
-- recursive step climbs ONLY through nested IDVs (`nested_idv`, ~1e4 rows) → the fan-out is a cheap
-- hash probe, no second 108M scan. Depth-capped at 12 (FPDS nesting is shallow) as a runaway guard;
-- parent_award_key_resolved is a single-valued tree edge, so there are no cross-path double-counts.
CREATE TEMP TABLE nested_idv AS
SELECT contract_award_unique_key AS idv_key, parent_award_key_resolved AS grandparent_key
FROM state_resolved
WHERE award_kind = 'idv'
  AND parent_award_key_resolved IS NOT NULL
  AND parent_award_key_resolved <> contract_award_unique_key;

CREATE TEMP TABLE idv_rollup AS
WITH RECURSIVE anc(award_key, ancestor_key, amt, depth) AS (
    SELECT contract_award_unique_key, parent_award_key_resolved, life_to_date_obligated, 1
    FROM state_resolved
    WHERE parent_award_key_resolved IS NOT NULL
      AND parent_award_key_resolved <> contract_award_unique_key   -- exclude self-parented roots
    UNION ALL
    SELECT a.award_key, n.grandparent_key, a.amt, a.depth + 1
    FROM anc a JOIN nested_idv n ON n.idv_key = a.ancestor_key      -- climb ONLY through nested IDVs
    WHERE a.depth < 12
)
SELECT ancestor_key AS idv_key,
       SUM(amt)                  AS idv_child_obligated,
       COUNT(DISTINCT award_key) AS idv_child_order_count           -- distinct descendants in the subtree
FROM anc GROUP BY ancestor_key;

-- IDVs that are themselves the resolved parent of another IDV (multi-tier: GWAC/FSS → BPA → orders).
-- With the recursive rollup the denominator is now EXACT even here; has_child_idv is retained as a
-- structural flag ("this vehicle has sub-vehicles beneath it"), no longer a lower-bound warning.
CREATE TEMP TABLE idv_with_child_idv AS
SELECT DISTINCT grandparent_key AS idv_key FROM nested_idv;

-- STATE final: capacity ratios. IDV consumption uses the child rollup; AWARD/ORDER use own life-to-date.
-- Ratios UNCLAMPED (>1 over-ceiling, <0 net de-obligated are legitimate signals). Time axis anchored to
-- the injected build_date literal (deterministic per build; the durable queue ranges on absolute
-- current_end_date, days_to_expiry is the same-day convenience axis).
CREATE TEMP TABLE state_final AS
SELECT r.contract_award_unique_key, r.award_kind, r.idv_type_code, r.award_id_piid,
       r.awarding_agency_code, r.awarding_sub_agency_code,
       r.recipient_uei, r.recipient_hash, r.recipient_name,
       r.naics_code, r.product_or_service_code, r.type_of_set_aside_code, r.award_type_code,
       r.solicitation_identifier,
       r.life_to_date_obligated, r.current_authorized_ceiling, r.current_total_value_of_award,
       r.potential_ceiling, r.potential_ceiling_is_fallback,
       r.total_dollars_obligated_snapshot, r.obligation_reconciliation_delta,
       CASE WHEN r.award_kind = 'idv' THEN COALESCE(ro.idv_child_obligated, 0.0) END AS idv_child_obligated,
       CASE WHEN r.award_kind = 'idv' THEN COALESCE(ro.idv_child_order_count, 0) END AS idv_child_order_count,
       CASE WHEN r.award_kind = 'idv' THEN (ci.idv_key IS NOT NULL) END              AS has_child_idv,
       (CASE WHEN r.award_kind = 'idv' THEN COALESCE(ro.idv_child_obligated, 0.0)
             ELSE r.life_to_date_obligated END) / NULLIF(r.potential_ceiling, 0)     AS consumed_pct,
       r.potential_ceiling - (CASE WHEN r.award_kind = 'idv' THEN COALESCE(ro.idv_child_obligated, 0.0)
                                   ELSE r.life_to_date_obligated END)                AS remaining_ceiling_headroom,
       date_diff('day', DATE '{build_date_iso}', r.current_end_date)                 AS days_to_expiry,
       r.is_terminated, r.terminal_action_type_code,
       (r.current_end_date IS NOT NULL AND r.current_end_date < DATE '{build_date_iso}'
         AND (r.terminal_action_type_code IS NULL
              OR r.terminal_action_type_code NOT IN ('E','F','X','K')))              AS is_expired_no_followon,
       r.ladder_txn_count, r.first_action_date, r.last_action_date,
       r.parent_award_id_piid, r.parent_award_agency_id,
       r.parent_award_key_resolved, r.parent_award_key_synth, r.parent_match_flag,
       r.current_end_date, r.potential_end_date, r.pop_start_date,
       r.canonical_source_final,
       DATE '{build_date_iso}'                                                       AS build_date
FROM state_resolved r
LEFT JOIN idv_rollup ro ON ro.idv_key = r.contract_award_unique_key
LEFT JOIN idv_with_child_idv ci ON ci.idv_key = r.contract_award_unique_key;

-- DELTA: one row per MODIFYING txn; the footprint-pruned first-difference. delta_federal_action_obligation
-- is THIS row's per-txn obligation (already a signed delta — NO lag). is_scope_increase keys off the
-- COMPUTED delta sign (never AF footprint membership) so civilian-agency signal is preserved.
CREATE TEMP TABLE delta_out AS
SELECT
    w.ctuk                                       AS contract_transaction_unique_key,
    w.cauk                                       AS contract_award_unique_key,
    w.action_date, w.last_modified_date, w.modification_number,
    w.award_kind, w.action_type_code,
    -- agency-aware calibration keys (Cycle 2): the origination engine scores every $ / rate signal by
    -- its percentile WITHIN (CGAC × parent/child-pool) — global thresholds misfire (Δceiling P99 spans
    -- 226x across agencies). awarding_agency_code binds the per-agency distribution; award_pool splits
    -- the IDV-parent ceiling mods from the order/definitive-child obligation mods BEFORE the percentile
    -- (pooling is what manufactures DoD's $81M artifact). See FPDS_L2_AGENCY_CALIBRATION.md.
    w.awarding_agency_code,
    CASE WHEN w.award_kind = 'idv' THEN 'parent' ELSE 'child' END AS award_pool,
    {_KLASS_CASE}                                AS action_type_klass,
    w.p_ctuk                                     AS prev_contract_transaction_unique_key,
    w.p_action_date                              AS prev_action_date,
    w.federal_action_obligation                  AS delta_federal_action_obligation,
    (w.current_total_value_of_award - w.p_ctv)   AS delta_current_total_value_of_award,
    (w.base_and_exercised_options_value - w.p_beo) AS delta_authorized_ceiling,
    (w.pot_val - w.p_pot)                        AS delta_potential_ceiling,
    (w.base_and_all_options_value - w.p_bao)     AS delta_base_and_all_options,
    (w.total_dollars_obligated - w.p_tdo)        AS delta_total_dollars_obligated,
    date_diff('day', w.p_pce, w.pop_cur_end)     AS delta_current_end_date_days,
    date_diff('day', w.p_pend, w.pot_end)        AS delta_potential_end_date_days,
    date_diff('day', w.p_ord, w.ord_end)         AS delta_ordering_end_date_days,
    date_diff('day', w.p_pstart, w.pop_start)    AS delta_pop_start_date_days,
    ( (w.recipient_uei IS DISTINCT FROM w.p_uei)
      OR (w.recipient_hash IS DISTINCT FROM w.p_hash)
      OR (w.recipient_name IS DISTINCT FROM w.p_name)
      OR (w.parent_uei IS DISTINCT FROM w.p_puei)
      OR (w.cage_code IS DISTINCT FROM w.p_cage)
      OR (w.business_categories IS DISTINCT FROM w.p_bizcat)
      OR (w.co_det IS DISTINCT FROM w.p_codet)
      OR (w.funding_office_code IS DISTINCT FROM w.p_foc)
      OR (w.recipient_county_name IS DISTINCT FROM w.p_county) )         AS identity_changed,
    concat_ws(',',
      CASE WHEN w.recipient_uei IS DISTINCT FROM w.p_uei THEN 'recipient_uei' END,
      CASE WHEN w.recipient_hash IS DISTINCT FROM w.p_hash THEN 'recipient_hash' END,
      CASE WHEN w.recipient_name IS DISTINCT FROM w.p_name THEN 'recipient_name' END,
      CASE WHEN w.parent_uei IS DISTINCT FROM w.p_puei THEN 'parent_uei' END,
      CASE WHEN w.cage_code IS DISTINCT FROM w.p_cage THEN 'cage_code' END,
      CASE WHEN w.business_categories IS DISTINCT FROM w.p_bizcat THEN 'business_categories' END,
      CASE WHEN w.co_det IS DISTINCT FROM w.p_codet
           THEN 'contracting_officers_determination_of_business_size' END,
      CASE WHEN w.funding_office_code IS DISTINCT FROM w.p_foc THEN 'funding_office_code' END,
      CASE WHEN w.recipient_county_name IS DISTINCT FROM w.p_county THEN 'recipient_county_name' END
    )                                            AS identity_change_fields,
    w.p_uei                                      AS prev_recipient_uei,
    w.recipient_uei,
    w.p_foc                                      AS prev_funding_office_code,
    -- is_scope_increase = the CEILING grew (new headroom / opportunity), kind-agnostic. Keys strictly
    -- off the potential/all-options ceiling delta — NOT current_total_value_of_award (obligated-to-date =
    -- CONSUMPTION, which grows on an option exercise WITHIN the existing ceiling). Consumption/mobilization
    -- is surfaced separately via action_type_klass='option_exercise' + delta_federal_action_obligation.
    (COALESCE(w.pot_val - w.p_pot, 0) > 0
     OR COALESCE(w.base_and_all_options_value - w.p_bao, 0) > 0)         AS is_scope_increase,
    (w.action_type_code IN ('E','F','X'))        AS is_termination_event,
    w.canonical_source,
    TIMESTAMP '{built_at_iso}'                    AS built_at
FROM win w
WHERE w.action_type_code IS NOT NULL AND w.action_type_code <> '';
"""


# ════════════════════════════════════════════════════════════════════════════════════════
# R2 publish / index — reuse the spine's battle-tested plumbing, per-table index plan
# ════════════════════════════════════════════════════════════════════════════════════════
def _build_indices_local(local_ds: str, btree: list[str], bitmap: list[str]) -> list[str]:
    """Build BTREE/BITMAP scalar indices against a LOCAL Lance path (local-FS writer — the ONLY
    R2-safe way; the native R2 object-writer streams adaptive parts R2 rejects: 400 InvalidPart).
    Presence-filtered. Raises on the first failing column → callers fail-closed BEFORE any R2 mutation."""
    import lance
    ds = lance.dataset(local_ds)                     # LOCAL — no storage_options, no R2 writer
    present = set(ds.schema.names)
    built: list[str] = []
    log(f"indexing LOCAL ({ds.count_rows():,} rows)")
    for col in [c for c in btree if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BTREE", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BTREE")
        built.append(col)
        log(f"  BTREE ✓ {col}")
    for col in [c for c in bitmap if c in present]:
        try:
            ds.create_scalar_index(col, index_type="BITMAP", replace=True)
        except TypeError:
            ds.create_scalar_index(col, index_type="BITMAP")
        built.append(col)
        log(f"  BITMAP ✓ {col}")
    return built


# The write phase (needs the open DuckDB con) and the index+publish phase (must run AFTER DuckDB is
# freed) are split DELIBERATELY: _build_indices_local runs an in-RAM scalar-index sort under
# LANCE_BYPASS_SPILLING, and a >=40M-row BTREE build colliding with a still-resident 72GB DuckDB arena
# is an out-of-band OOM-SIGKILL the except/finally CANNOT catch (→ no ledger row). Mirrors the spine
# build() fold: write data-only local → close+trim DuckDB → index+publish.
def _write_local(con, *, select_sql: str, local_ds: str) -> None:
    """Stream a DuckDB relation → LOCAL data-only Lance (no indices). Runs while con is open."""
    import lance
    shutil.rmtree(local_ds, ignore_errors=True)
    reader = con.sql(select_sql).to_arrow_reader(batch_size=200_000)
    log(f"writing Lance LOCALLY → {local_ds}")
    lance.write_dataset(reader, local_ds, mode="overwrite",
                        data_storage_version=DATA_STORAGE_VERSION,
                        max_rows_per_file=MAX_ROWS_PER_FILE, max_bytes_per_file=MAX_BYTES_PER_FILE)
    del reader


def _index_publish(*, local_ds: str, target_uri: str, btree: list[str],
                   bitmap: list[str]) -> tuple[int, list[str]]:
    """Build §4 indices on the LOCAL dataset (con-free) → single boto3 uniform-part publish (data +
    indices atomically). Returns (files_published, indices_built)."""
    built = _build_indices_local(local_ds, btree, bitmap)
    log(f"indices built LOCALLY: {built}")
    s3 = _spine._s3()
    log(f"publishing local Lance (data + indices) → {target_uri} (boto3 uniform-part)…")
    published = _spine._publish_local_to_r2(s3, target_uri, local_ds)
    log(f"published {published} files → {target_uri}")
    return published, built


def index(table: str, target_uri: str) -> dict:
    """Standalone R2-safe index (repair path; build() already folds indexing in). Mirror → local →
    LOCAL-FS index build → append-only delta publish → orphan-index GC. Blast-radius isolated: a
    failed index never rewrites data fragments. Reuses the spine's mirror/delta/GC helpers verbatim."""
    btree, bitmap = (STATE_BTREE, STATE_BITMAP) if table == "state" else (DELTA_BTREE, DELTA_BITMAP)
    s3 = _spine._s3()
    local_ds = os.path.join(SCRATCH, f"index_{table}_lance")
    shutil.rmtree(local_ds, ignore_errors=True)
    os.makedirs(local_ds, exist_ok=True)
    try:
        log(f"materializing {target_uri} → {local_ds} (boto3 mirror)…")
        got = _spine._download_r2_to_local(s3, target_uri, local_ds)
        log(f"materialized {got} files")
        before = _spine._relset(local_ds)
        built = _build_indices_local(local_ds, btree, bitmap)
        after = _spine._relset(local_ds)
        published = _spine._publish_index_delta(s3, target_uri, local_ds, before)
        keep = {rel[len("_indices/"):].split("/", 1)[0]
                for rel in (after - before) if rel.startswith("_indices/")}
        pruned = _spine._gc_orphan_indices(s3, target_uri, keep) if keep else 0
        log(f"indices built: {built} (orphan index objects pruned: {pruned})")
        return {"table": table, "target_uri": target_uri, "indices_built": built,
                "files_published": published, "orphans_pruned": pruned}
    finally:
        shutil.rmtree(local_ds, ignore_errors=True)


# ════════════════════════════════════════════════════════════════════════════════════════
# ops.* ledger
# ════════════════════════════════════════════════════════════════════════════════════════
def init_ops() -> None:
    import psycopg
    from pathlib import Path
    sql = Path(__file__).parent.joinpath(OPS_SQL_FILE).read_text()
    with psycopg.connect(os.environ["HQX_DB_URL_POOLED"]) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    log("L2 ops DDL applied")


def _record_run(ops_table: str, feed: str, cols: dict) -> None:
    """One ledger row per (table, build). Self-bootstraps the DDL via to_regclass on first write.
    error_message is NEVER null when status<>success. Audit failure never masks the build."""
    import psycopg
    from pathlib import Path
    dsn = os.environ.get("HQX_DB_URL_POOLED")
    if not dsn:
        log("WARN: HQX_DB_URL_POOLED not set; skipping ops.* write.")
        return
    if cols.get("status") != "success" and not cols.get("error_message"):
        cols["error_message"] = "unknown terminal failure (no exception captured)"
    cols = {"feed": feed, **cols}
    names = list(cols.keys())
    placeholders = ",".join(["%s"] * len(names))
    try:
        with psycopg.connect(dsn) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{ops_table}')")
            if cur.fetchone()[0] is None:
                cur.execute(Path(__file__).parent.joinpath(OPS_SQL_FILE).read_text())
            cur.execute(f"INSERT INTO {ops_table} ({','.join(names)}) VALUES ({placeholders})",
                        [cols[n] for n in names])
            conn.commit()
    except Exception as exc:  # noqa: BLE001 — audit must not mask the build
        log(f"WARN: ops.* write failed ({ops_table}): {exc}")


# ════════════════════════════════════════════════════════════════════════════════════════
# build — the shared pass → both tables
# ════════════════════════════════════════════════════════════════════════════════════════
def build(since: str | None = None, state_uri: str = STATE_URI, delta_uri: str = DELTA_URI,
          build_date: str | None = None) -> dict:
    import ctypes
    import gc as _gc

    import lance

    started = dt.datetime.now(dt.timezone.utc)
    built_at_iso = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S.%f")
    build_date_iso = build_date or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    so = _spine._r2_so()

    state_status, delta_status, error = "error", "error", None
    state_metrics: dict = {}
    delta_metrics: dict = {}
    con = None
    state_local = os.path.join(SCRATCH, "state_lance")
    delta_local = os.path.join(SCRATCH, "delta_lance")
    try:
        os.makedirs(SCRATCH, exist_ok=True)
        # --since pushed into the single spine scanner (spine action_date is date32 → DATE literal).
        spine_filter = f"action_date >= DATE '{since}'" if since else None
        spine_ds = lance.dataset(SPINE_URI, storage_options=so)
        spine_manifest_version = int(spine_ds.version)
        present = set(spine_ds.schema.names)
        scan_cols = [c for c in SPINE_SCAN_COLS if c in present]
        missing = [c for c in SPINE_SCAN_COLS if c not in present]
        if missing:
            raise RuntimeError(f"spine is missing required L2 source columns: {missing}")

        con = _duck()
        log(f"registering spine (since={since}, manifest v{spine_manifest_version}) → "
            f"state={state_uri} delta={delta_uri}")
        con.register("spine_src", spine_ds.scanner(columns=scan_cols, filter=spine_filter).to_reader())

        con.execute(_projection_sql())            # single R2 pass → spine_proj temp table
        rows_in_spine = con.execute("SELECT count(*) FROM spine_proj").fetchone()[0]
        con.execute(_stage_sql(build_date_iso, built_at_iso))   # the ONE window + both branch tables

        # ── STATE metrics + fail-closed PK gate (one row per award key) ──
        s_total, s_distinct = con.execute(
            "SELECT count(*), count(DISTINCT contract_award_unique_key) FROM state_final").fetchone()
        if s_total != s_distinct:
            raise RuntimeError(f"STATE PK gate FAILED: count(*)={s_total:,} != "
                               f"distinct(contract_award_unique_key)={s_distinct:,}. Aborting publish.")
        kind_counts = dict(con.execute(
            "SELECT award_kind, count(*) FROM state_final GROUP BY 1").fetchall())
        dangling_count, dangling_oblig = con.execute(
            "SELECT count(*), COALESCE(SUM(life_to_date_obligated),0) FROM state_final "
            "WHERE parent_match_flag='dangling'").fetchone()
        recon_p99 = con.execute(
            "SELECT quantile_cont(abs(obligation_reconciliation_delta), 0.99) FROM state_final "
            "WHERE obligation_reconciliation_delta IS NOT NULL").fetchone()[0]
        recon_fail = con.execute(
            "SELECT count(*) FROM state_final WHERE abs(obligation_reconciliation_delta) > %s"
            % RECON_TOLERANCE).fetchone()[0]
        max_current_end = con.execute("SELECT max(current_end_date) FROM state_final").fetchone()[0]

        # ── DELTA metrics + fail-closed PK gate (one row per mod txn) ──
        d_total, d_distinct = con.execute(
            "SELECT count(*), count(DISTINCT contract_transaction_unique_key) FROM delta_out").fetchone()
        if d_total != d_distinct:
            raise RuntimeError(f"DELTA PK gate FAILED: count(*)={d_total:,} != "
                               f"distinct(contract_transaction_unique_key)={d_distinct:,}. Aborting publish.")
        d_awards = con.execute("SELECT count(DISTINCT contract_award_unique_key) FROM delta_out").fetchone()[0]
        novation_ct = con.execute("SELECT count(*) FROM delta_out WHERE action_type_code='J'").fetchone()[0]
        termination_ct = con.execute("SELECT count(*) FROM delta_out WHERE is_termination_event").fetchone()[0]
        scope_inc_ct = con.execute("SELECT count(*) FROM delta_out WHERE is_scope_increase").fetchone()[0]
        identity_ct = con.execute("SELECT count(*) FROM delta_out WHERE identity_changed").fetchone()[0]
        pot_null_rate = con.execute(
            "SELECT COALESCE(avg(CASE WHEN delta_potential_ceiling IS NULL THEN 1.0 ELSE 0.0 END),0) "
            "FROM delta_out").fetchone()[0]

        log(f"gates PASSED — awards_out={s_total:,} ({kind_counts}) mods_out={d_total:,} "
            f"dangling={dangling_count:,} recon_fail={recon_fail:,}")

        # ── write BOTH tables to LOCAL data-only Lance while con is open ──
        _write_local(con, select_sql="SELECT * FROM state_final", local_ds=state_local)
        _write_local(con, select_sql="SELECT * FROM delta_out", local_ds=delta_local)

        # ── free DuckDB (RSS + spill) BEFORE the RAM-heavy in-RAM index sorts. glibc does not return
        #    freed arenas to the OS on its own → malloc_trim forces it, so a resident 72GB DuckDB heap
        #    cannot collide with the LANCE_BYPASS_SPILLING BTREE build and trigger an out-of-band
        #    OOM-SIGKILL (which the except/finally cannot catch). Mirrors the spine build() fold. ──
        con.close(); con = None
        del spine_ds
        _gc.collect()
        try:
            ctypes.CDLL("libc.so.6").malloc_trim(0)
        except OSError:
            pass  # non-glibc (macOS dev)
        shutil.rmtree(DUCK_TMP, ignore_errors=True)

        # ── index + publish each table (con-free); assemble each ledger dict AS SOON AS it lands, so a
        #    later failure records TRUE per-table lineage rather than a success-with-NULL-metrics row. ──
        state_pub, state_idx = _index_publish(local_ds=state_local, target_uri=state_uri,
                                              btree=STATE_BTREE, bitmap=STATE_BITMAP)
        state_status = "success"
        state_metrics = {
            "spine_manifest_version": spine_manifest_version, "build_date": build_date_iso,
            "rows_in_spine": int(rows_in_spine), "awards_out": int(s_total),
            "definitive_count": int(kind_counts.get("definitive", 0)),
            "idv_count": int(kind_counts.get("idv", 0)), "order_count": int(kind_counts.get("order", 0)),
            "dangling_count": int(dangling_count),
            "dangling_child_obligated_total": float(dangling_oblig or 0.0),
            "recon_delta_p99": float(recon_p99 or 0.0), "recon_fail_count": int(recon_fail),
            "max_current_end_date": max_current_end, "files_published": int(state_pub),
            "indices_built": state_idx, "columns": None, "status": state_status}

        delta_pub, delta_idx = _index_publish(local_ds=delta_local, target_uri=delta_uri,
                                              btree=DELTA_BTREE, bitmap=DELTA_BITMAP)
        delta_status = "success"
        delta_metrics = {
            "spine_manifest_version": spine_manifest_version, "build_date": build_date_iso,
            "mods_out": int(d_total), "distinct_awards": int(d_awards),
            "novation_count": int(novation_ct), "termination_count": int(termination_ct),
            "scope_increase_count": int(scope_inc_ct), "identity_change_count": int(identity_ct),
            "potential_delta_null_rate": float(pot_null_rate or 0.0), "files_published": int(delta_pub),
            "indices_built": delta_idx, "status": delta_status}
        log(f"DONE → state rows={s_total:,} delta rows={d_total:,}")
        return {"state": state_metrics, "delta": delta_metrics, "since": since,
                "build_date": build_date_iso, "spine_manifest_version": spine_manifest_version}
    except BaseException as exc:  # noqa: BLE001
        error = f"{type(exc).__name__}: {exc}" if str(exc) else f"{type(exc).__name__} (no message)"
        log(f"FAILED: {error}")
        raise
    finally:
        if con is not None:
            con.close()
        completed = dt.datetime.now(dt.timezone.utc)
        _record_run(STATE_OPS_TABLE, STATE_FEED, {
            **{k: state_metrics.get(k) for k in (
                "spine_manifest_version", "build_date", "rows_in_spine", "awards_out",
                "definitive_count", "idv_count", "order_count", "dangling_count",
                "dangling_child_obligated_total", "recon_delta_p99", "recon_fail_count",
                "max_current_end_date")},
            "write_mode": "overwrite",
            "indices_built": ",".join(state_metrics.get("indices_built") or []) or None,
            "status": state_status, "error_message": None if state_status == "success" else error,
            "started_at": started, "completed_at": completed})
        _record_run(DELTA_OPS_TABLE, DELTA_FEED, {
            **{k: delta_metrics.get(k) for k in (
                "spine_manifest_version", "build_date", "mods_out", "distinct_awards",
                "novation_count", "termination_count", "scope_increase_count", "identity_change_count",
                "potential_delta_null_rate")},
            "write_mode": "overwrite",
            "indices_built": ",".join(delta_metrics.get("indices_built") or []) or None,
            "status": delta_status, "error_message": None if delta_status == "success" else error,
            "started_at": started, "completed_at": completed})
        shutil.rmtree(SCRATCH, ignore_errors=True)


# ════════════════════════════════════════════════════════════════════════════════════════
# verify — read-back structural assertions per table
# ════════════════════════════════════════════════════════════════════════════════════════
def verify(table: str, target_uri: str) -> dict:
    """Independent scanner → DuckDB. Gates: PK-unique; award_kind ⊆ {definitive,idv,order};
    (state) consumed_pct sanity + parent_match_flag domain; (delta) action_type_klass domain +
    built_at distinct==1. Returns a JSON verdict with a `pass` bool + `failures` list."""
    import lance
    so = _spine._r2_so()
    ds = lance.dataset(target_uri, storage_options=so)
    try:
        idx = [i.get("name") if isinstance(i, dict) else getattr(i, "name", str(i))
               for i in ds.list_indices()]
    except Exception:  # noqa: BLE001
        idx = []
    con = _duck()
    con.register("c_src", ds.scanner().to_reader())
    con.execute("CREATE TEMP TABLE c AS SELECT * FROM c_src")
    failures: list[str] = []
    rows = con.execute("SELECT count(*) FROM c").fetchone()[0]
    kinds = dict(con.execute("SELECT award_kind, count(*) FROM c GROUP BY 1").fetchall())
    if set(kinds) - {"definitive", "idv", "order"}:
        failures.append(f"award_kind domain leak: {set(kinds) - {'definitive','idv','order'}}")

    if table == "state":
        pk = "contract_award_unique_key"
        total, distinct = con.execute(
            f"SELECT count(*), count(DISTINCT {pk}) FROM c").fetchone()
        if total != distinct:
            failures.append(f"PK not unique: {total:,} rows, {distinct:,} distinct {pk}")
        flags = dict(con.execute("SELECT parent_match_flag, count(*) FROM c GROUP BY 1").fetchall())
        if set(flags) - {"self", "resolved", "dangling"}:
            failures.append(f"parent_match_flag domain leak: {set(flags)}")
        # every 'order' resolves to a real IDV key or is flagged dangling (never silently self-rooted)
        bad_order = con.execute(
            "SELECT count(*) FROM c WHERE award_kind='order' AND parent_match_flag='self'").fetchone()[0]
        if bad_order:
            failures.append(f"{bad_order:,} 'order' rows mis-flagged 'self'")
        report = {"rows": rows, "kinds": kinds, "parent_match_flag": flags}
    else:
        pk = "contract_transaction_unique_key"
        total, distinct = con.execute(
            f"SELECT count(*), count(DISTINCT {pk}) FROM c").fetchone()
        if total != distinct:
            failures.append(f"PK not unique: {total:,} rows, {distinct:,} distinct {pk}")
        base = con.execute(
            "SELECT count(*) FROM c WHERE action_type_code IS NULL OR action_type_code=''").fetchone()[0]
        if base:
            failures.append(f"{base:,} base rows leaked into DELTA (should be mods only)")
        built_at_distinct = con.execute("SELECT count(DISTINCT built_at) FROM c").fetchone()[0]
        if rows and built_at_distinct != 1:   # 0 on a legitimately-empty delta table is not a violation
            failures.append(f"built_at not a single literal: {built_at_distinct} distinct")
        klass = dict(con.execute("SELECT action_type_klass, count(*) FROM c GROUP BY 1").fetchall())
        report = {"rows": rows, "kinds": kinds, "action_type_klass": klass}
    con.close()
    verdict = {"table": table, "target_uri": target_uri, "indices": idx,
               "pass": not failures, "failures": failures, **report}
    return verdict


# ════════════════════════════════════════════════════════════════════════════════════════
def _arg_val(flag: str, argv: list[str], default: str | None = None) -> str | None:
    if flag in argv:
        i = argv.index(flag)
        if i + 1 < len(argv):
            return argv[i + 1]
    return default


def main():
    argv = sys.argv[1:]
    cmd = argv[0] if argv else "verify"
    state_uri = _arg_val("--state-uri", argv, STATE_URI)
    delta_uri = _arg_val("--delta-uri", argv, DELTA_URI)
    table = _arg_val("--table", argv, None)
    if cmd == "init_ops":
        init_ops()
    elif cmd == "build":
        since = _arg_val("--since", argv, None)
        build_date = _arg_val("--build-date", argv, None)
        print(json.dumps(build(since=since, state_uri=state_uri, delta_uri=delta_uri,
                               build_date=build_date), indent=2, default=str))
    elif cmd == "index":
        if table not in ("state", "delta"):
            print("index requires --table state|delta"); sys.exit(2)
        uri = state_uri if table == "state" else delta_uri
        print(json.dumps(index(table=table, target_uri=uri), indent=2, default=str))
    elif cmd == "verify":
        if table not in ("state", "delta"):
            print("verify requires --table state|delta"); sys.exit(2)
        uri = state_uri if table == "state" else delta_uri
        print(json.dumps(verify(table=table, target_uri=uri), indent=2, default=str))
    elif cmd == "print_sql":
        build_date = _arg_val("--build-date", argv, "2026-07-03")
        print(_projection_sql())
        print(_stage_sql(build_date, "2026-07-03 00:00:00.000000"))
    else:
        print(f"unknown command: {cmd} (init_ops|build|index|verify|print_sql)")
        sys.exit(2)


if __name__ == "__main__":
    main()
