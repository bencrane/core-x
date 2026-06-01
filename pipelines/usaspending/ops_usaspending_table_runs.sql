-- Terminal-state table for the USAspending bulk ingest worker.
-- Written by pipelines/usaspending/usaspending_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every Phase 2 terminal state, success or failure —
-- mirrors the ops.* contract used by the SAM / PPP feeds (ARCHITECTURE.md §5).
-- One row per (snapshot, table) ingest attempt. Phase 1 staging is verified via
-- R2 object listing, not ops. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.usaspending_table_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    snapshot_date  date        NOT NULL,   -- point-in-time dump stamp, e.g. 2026-05-06
    dump_id        text        NOT NULL,   -- pg_dump TOC id, e.g. 5968
    schema_name    text        NOT NULL,   -- source pg schema: rpt | raw | public | int
    table_name     text        NOT NULL,   -- source pg table, e.g. transaction_search_fpds
    landing_key    text,                   -- s3://data-sink/landing/usaspending/<stamp>/<...>.dat.gz
    size_class     text,                   -- 'standard' | 'giant'
    member_bytes   bigint,                 -- gzip member size (== stored byte length)
    rows_processed bigint,
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS usaspending_table_runs_table_idx
    ON ops.usaspending_table_runs (schema_name, table_name);
CREATE INDEX IF NOT EXISTS usaspending_table_runs_status_idx
    ON ops.usaspending_table_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_table_runs_snapshot_idx
    ON ops.usaspending_table_runs (snapshot_date);
CREATE INDEX IF NOT EXISTS usaspending_table_runs_recorded_at_idx
    ON ops.usaspending_table_runs (recorded_at DESC);
