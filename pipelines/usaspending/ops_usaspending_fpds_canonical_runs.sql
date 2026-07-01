-- Ops ledger for the USAspending FPDS CANONICAL transaction table build (typed v2 SoR).
-- Reconciles BULK (transaction_search_fpds) + FRESH (contract_prime_txn) + MONTHLY (monthly
-- bulk-download CSV; physical upstream still named usaspending_archive_*_fpds).
-- Two-tier: TIER1 bulk_base = BULK⊕MONTHLY (tie→MONTHLY), TIER2 core = bulk_base⊕FRESH (tie→FRESH);
-- executed as one flat argmax(last_modified_date) window per transaction key (source_rank
-- FRESH>MONTHLY>BULK) — monthly competes on shared keys so monthly-CSV corrections land. Enrichment
-- is pg-preferred COALESCE(BULK, MONTHLY) fill (monthly-unique TAS/federal + officer-comp cols).
-- Then R6-scoped tombstone (latest archive_snapshot_stamp) minus R5 reinstatement of newer 'D' keys.
-- Overwrite read-model rebuild. One row per terminal state, written by the worker's _record_run via
-- psycopg (HQX_DB_URL_POOLED). Idempotent DDL.
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
    monthly_corrections_applied bigint,            -- monthly core-wins on keys also held by BULK (landed corrections)
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
-- Idempotent forward-fill: CREATE TABLE IF NOT EXISTS never ALTERs an existing table, so schemas
-- initialized before this column existed pick it up here. archive→monthly rename: RENAME the legacy
-- column in place when present (preserves history), THEN ensure the new column exists for schemas
-- that never had the legacy one. Both guarded (IF EXISTS / IF NOT EXISTS) → idempotent + order-safe.
ALTER TABLE ops.usaspending_fpds_canonical_runs
    RENAME COLUMN IF EXISTS archive_corrections_applied TO monthly_corrections_applied;
ALTER TABLE ops.usaspending_fpds_canonical_runs
    ADD COLUMN IF NOT EXISTS monthly_corrections_applied bigint;
CREATE INDEX IF NOT EXISTS usaspending_fpds_canonical_runs_status_idx
    ON ops.usaspending_fpds_canonical_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_fpds_canonical_runs_recorded_at_idx
    ON ops.usaspending_fpds_canonical_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS usaspending_fpds_canonical_runs_max_action_date_idx
    ON ops.usaspending_fpds_canonical_runs (max_action_date DESC);
