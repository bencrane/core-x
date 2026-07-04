-- Ops ledger for the USAspending FPDS ENTITY VERSION dimension (SCD2 history core; Cycle 3).
-- Written by usaspending_fpds_entity_version.py's _record_run via psycopg (HQX_DB_URL_POOLED):
-- one row per build. Overwrite-rebuild read-model. Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.usaspending_fpds_entity_version_runs (
    id                              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                            text        NOT NULL,   -- 'usaspending_fpds_entity_version'
    spine_manifest_version          bigint,                 -- L1 spine Lance version this build read
    build_date                      date,                   -- injected build_date literal
    rows_with_uei                   bigint,                 -- spine txns in UEI-era scope (post --since)
    distinct_uei                    bigint,                 -- distinct recipient_uei versioned
    versions_out                    bigint,                 -- version rows written (== distinct entity_version_key)
    initial_version_count           bigint,                 -- change_klass='initial' (== distinct_uei)
    new_award_observation_count     bigint,                 -- boundary observed on a base-award record
    novation_boundary_count         bigint,                 -- change_action_type_code='J'
    smoothed_txn_count              bigint,                 -- single-txn-reversion blips folded
    multi_version_entity_count      bigint,                 -- UEIs with >1 version
    max_versions_per_entity         bigint,
    write_mode                      text,                   -- 'overwrite'
    indices_built                   text,                   -- comma list of BTREE/BITMAP columns
    status                          text        NOT NULL,   -- 'success' | 'error'
    error_message                   text,                   -- NEVER null when status<>'success'
    started_at                      timestamptz,
    completed_at                    timestamptz,
    recorded_at                     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS usaspending_fpds_entity_version_runs_status_idx
    ON ops.usaspending_fpds_entity_version_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_fpds_entity_version_runs_recorded_at_idx
    ON ops.usaspending_fpds_entity_version_runs (recorded_at DESC);
