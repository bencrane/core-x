-- Terminal-state table for the SEC EDGAR structured-data cluster
-- (CIK Identity Spine + Form 4 / Insider Transactions + Form D).
-- Written by pipelines/edgar/ingest.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / SBA / USPTO feeds (ARCHITECTURE.md §5).
-- Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.edgar_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset          text        NOT NULL,   -- 'cik_map' | 'form_4' | 'form_d'
    feed             text        NOT NULL,   -- 'edgar_cik_map' | 'edgar_form_4' | 'edgar_form_d'
    run_mode         text        NOT NULL,   -- 'cik_map' | 'backfill' | 'quarter' | 'reindex'
    write_mode       text,                   -- 'overwrite' | 'overwrite+append' | 'merge_insert' | 'create'
    dataset_uri      text,                   -- s3://data-sink/active/edgar_*/
    quarter          text,                   -- 'YYYYqQ' (single quarter) | 'backfill' | 'weekly'
    source_files     jsonb,                  -- list of source urls / zip basenames processed this run
    quarters_processed integer,              -- quarters streamed (1 for a single-quarter run; NULL for cik_map)
    rows_processed   bigint,                 -- rows projected out of DuckDB this run
    rows_written     bigint,                 -- total rows in the Lance dataset after the write
    status           text        NOT NULL,   -- 'success' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS edgar_runs_dataset_idx     ON ops.edgar_runs (dataset);
CREATE INDEX IF NOT EXISTS edgar_runs_feed_idx        ON ops.edgar_runs (feed);
CREATE INDEX IF NOT EXISTS edgar_runs_status_idx      ON ops.edgar_runs (status);
CREATE INDEX IF NOT EXISTS edgar_runs_quarter_idx     ON ops.edgar_runs (quarter DESC);
CREATE INDEX IF NOT EXISTS edgar_runs_recorded_at_idx ON ops.edgar_runs (recorded_at DESC);
