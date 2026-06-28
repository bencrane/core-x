-- Ops ledger for the USAspending FPDS CANONICAL transaction table build (typed v2 SoR).
-- Reconciles BULK (transaction_search_fpds) + FRESH (contract_prime_txn) + archive_full,
-- MAX(last_modified_date) per transaction key (tie-break FRESH), then ANTI-JOIN out
-- archive_delta correction_delete_ind='D'. Overwrite read-model rebuild. One row per terminal
-- state, written by the worker's _record_run via psycopg (HQX_DB_URL_POOLED). Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.usaspending_fpds_canonical_runs (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                   text        NOT NULL,   -- 'usaspending_fpds_canonical_txn'
    rows_in_bulk           bigint,                 -- input rows scanned from BULK (transaction_search_fpds)
    rows_in_fresh          bigint,                 -- input rows scanned from FRESH (contract_prime_txn)
    rows_in_archive_full   bigint,                 -- input rows scanned from archive Full
    rows_out               bigint,                 -- canonical rows written (post-dedup, post-tombstone)
    dedup_collapsed        bigint,                 -- rows dropped by MAX(last_modified_date) per-key collapse
    fresh_only_tail        bigint,                 -- keys present ONLY in FRESH (the freshness tail beyond BULK)
    deletes_tombstoned     bigint,                 -- rows anti-joined out via archive_delta 'D' ledger
    max_action_date        date,                   -- max(action_date) in the written canonical
    columns                integer,                -- typed column count written
    write_mode             text,                   -- 'overwrite'
    indices_built          text,                   -- comma list of BTREE/BITMAP columns
    status                 text        NOT NULL,   -- 'success' | 'error'
    error_message          text,                   -- NEVER null when status<>'success'
    started_at             timestamptz,
    completed_at           timestamptz,
    recorded_at            timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS usaspending_fpds_canonical_runs_status_idx
    ON ops.usaspending_fpds_canonical_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_fpds_canonical_runs_recorded_at_idx
    ON ops.usaspending_fpds_canonical_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS usaspending_fpds_canonical_runs_max_action_date_idx
    ON ops.usaspending_fpds_canonical_runs (max_action_date DESC);
