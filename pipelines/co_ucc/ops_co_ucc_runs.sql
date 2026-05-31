-- Terminal-state table for the Colorado UCC transaction-ledger bulk ingest worker.
-- Written by pipelines/co_ucc/transactions_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / SBA feeds (ARCHITECTURE.md §5). Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it via `modal run pipelines/co_ucc/transactions_bulk.py::init_ops`.
-- Keep the two in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.co_ucc_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,   -- 'co_ucc_transactions'
    dataset_uri    text        NOT NULL,   -- s3://data-sink/active/co_ucc_transactions/
    snapshot_date  date,                   -- as-of date decoded from the file stamp
    source_key     text,                   -- R2 landing key ingested
    source_file    text,                   -- basename of source_key
    rows_processed bigint,
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS co_ucc_runs_feed_idx        ON ops.co_ucc_runs (feed);
CREATE INDEX IF NOT EXISTS co_ucc_runs_status_idx      ON ops.co_ucc_runs (status);
CREATE INDEX IF NOT EXISTS co_ucc_runs_snapshot_idx    ON ops.co_ucc_runs (snapshot_date);
CREATE INDEX IF NOT EXISTS co_ucc_runs_recorded_at_idx ON ops.co_ucc_runs (recorded_at DESC);
