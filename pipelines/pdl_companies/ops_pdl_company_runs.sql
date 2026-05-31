-- Terminal-state table for the PDL Free Company Dataset bulk ingest worker.
-- Written by pipelines/pdl_companies/free_company_dataset.py:_record_run via
-- psycopg (HQX_DB_URL_POOLED) on every terminal state, success or failure —
-- mirrors the ops.* contract used by the SAM / SBA / SoS / UCC feeds
-- (ARCHITECTURE.md §5). Idempotent DDL. Also created at runtime by the worker's
-- init_schema (modal run ...::initdb); this file is the reviewable source.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pdl_company_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,    -- 'pdl_companies'
    dataset_uri     text        NOT NULL,    -- s3://data-sink/active/pdl_companies/
    source_file     text        NOT NULL,    -- free_company_dataset.pipe.zip (gzip content)
    landing_key     text        NOT NULL,    -- landing/pdl_companies/free_company_dataset.pipe.zip
    snapshot_date   date,                     -- ingest UTC date (manual-drop snapshot)
    rows_processed  bigint,                   -- committed Lance row count
    distinct_ids    bigint,                   -- exact COUNT(DISTINCT pdl_company_id); NULL on stream path
    write_path      text,                     -- 'materialize' | 'stream'
    status          text        NOT NULL,     -- 'success' | 'error'
    error           text,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS pdl_company_runs_status_idx        ON ops.pdl_company_runs (status);
CREATE INDEX IF NOT EXISTS pdl_company_runs_snapshot_date_idx ON ops.pdl_company_runs (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS pdl_company_runs_completed_at_idx  ON ops.pdl_company_runs (completed_at DESC);
