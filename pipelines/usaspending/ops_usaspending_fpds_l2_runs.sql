-- Ops ledgers for the USAspending FPDS L2 satellites (derived read-models over the L1 spine
-- usaspending_fpds_canonical_txn). Both tables are written by usaspending_fpds_l2.py's _record_run
-- via psycopg (HQX_DB_URL_POOLED): one row per (table, build). Overwrite-rebuild read-models.
-- Idempotent DDL (CREATE ... IF NOT EXISTS + ADD COLUMN IF NOT EXISTS forward-fill).
CREATE SCHEMA IF NOT EXISTS ops;

-- ── A) prime-award CAPACITY/STATE MV (award grain: contract_award_unique_key) ──────────────
CREATE TABLE IF NOT EXISTS ops.usaspending_fpds_prime_award_state_runs (
    id                              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                            text        NOT NULL,   -- 'usaspending_fpds_prime_award_state'
    spine_manifest_version          bigint,                 -- L1 spine Lance version this build read (lineage)
    build_date                      date,                   -- injected build_date literal (time-axis anchor)
    rows_in_spine                   bigint,                 -- spine transaction rows scanned (post --since)
    awards_out                      bigint,                 -- award-grain rows written (== distinct award keys)
    standalone_count                bigint,                 -- award_topology='standalone'
    vehicle_count                   bigint,                 -- award_topology='vehicle'
    vehicle_order_count             bigint,                 -- award_topology='vehicle_order'
    dangling_count                  bigint,                 -- orders whose parent IDV key did NOT resolve
    dangling_child_obligated_total  double precision,       -- Σ life_to_date of dangling orders (quarantine leak)
    recon_delta_p99                 double precision,       -- p99 |life_to_date − total_dollars_obligated_snapshot|
    recon_fail_count                bigint,                 -- rows with |recon delta| > tolerance ($1)
    max_current_end_date            date,                   -- max(current_end_date) written
    columns                         integer,                -- typed column count written (optional)
    write_mode                      text,                   -- 'overwrite'
    indices_built                   text,                   -- comma list of BTREE/BITMAP columns
    status                          text        NOT NULL,   -- 'success' | 'error'
    error_message                   text,                   -- NEVER null when status<>'success'
    started_at                      timestamptz,
    completed_at                    timestamptz,
    recorded_at                     timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE ops.usaspending_fpds_prime_award_state_runs
    ADD COLUMN IF NOT EXISTS dangling_child_obligated_total double precision;
-- award_kind→award_topology retonym (Layer 2): topology values renamed vehicle/vehicle_order/standalone.
-- Old definitive_count/idv_count/order_count columns retained for historical rows; new rows write these.
ALTER TABLE ops.usaspending_fpds_prime_award_state_runs
    ADD COLUMN IF NOT EXISTS standalone_count    bigint;
ALTER TABLE ops.usaspending_fpds_prime_award_state_runs
    ADD COLUMN IF NOT EXISTS vehicle_count       bigint;
ALTER TABLE ops.usaspending_fpds_prime_award_state_runs
    ADD COLUMN IF NOT EXISTS vehicle_order_count bigint;
CREATE INDEX IF NOT EXISTS usaspending_fpds_prime_award_state_runs_status_idx
    ON ops.usaspending_fpds_prime_award_state_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_fpds_prime_award_state_runs_recorded_at_idx
    ON ops.usaspending_fpds_prime_award_state_runs (recorded_at DESC);

-- ── B) MOD-DELTA / KINETIC MV (mod grain: contract_transaction_unique_key) ──────────────────
CREATE TABLE IF NOT EXISTS ops.usaspending_fpds_mod_delta_runs (
    id                              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                            text        NOT NULL,   -- 'usaspending_fpds_mod_delta'
    spine_manifest_version          bigint,                 -- L1 spine Lance version this build read
    build_date                      date,                   -- injected build_date literal
    mods_out                        bigint,                 -- modifying-transaction rows written (== distinct txn keys)
    distinct_awards                 bigint,                 -- distinct contract_award_unique_key touched by a mod
    novation_count                  bigint,                 -- action_type_code='J'
    termination_count               bigint,                 -- is_termination_event (E/F/X)
    scope_increase_count            bigint,                 -- is_scope_increase (computed positive ceiling delta)
    identity_change_count           bigint,                 -- identity_changed
    potential_delta_null_rate       double precision,       -- share of rows w/ NULL delta_potential_ceiling
                                                            --   (BULK-only feed → FRESH-provenance NULLs)
    columns                         integer,
    write_mode                      text,                   -- 'overwrite'
    indices_built                   text,
    status                          text        NOT NULL,   -- 'success' | 'error'
    error_message                   text,                   -- NEVER null when status<>'success'
    started_at                      timestamptz,
    completed_at                    timestamptz,
    recorded_at                     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS usaspending_fpds_mod_delta_runs_status_idx
    ON ops.usaspending_fpds_mod_delta_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_fpds_mod_delta_runs_recorded_at_idx
    ON ops.usaspending_fpds_mod_delta_runs (recorded_at DESC);
