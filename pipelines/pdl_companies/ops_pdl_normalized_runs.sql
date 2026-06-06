-- Terminal-state table for the pdl_normalized_companies derived-sidecar build worker.
-- Written by pipelines/pdl_companies/pdl_normalized_companies.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure. Mirrors the ops.* contract
-- (ARCHITECTURE.md §5) and ops.pdl_company_runs. Idempotent DDL. Also created at runtime by the
-- worker's init_schema (modal run ...::initdb); this file is the reviewable source.
--
-- source_version / source_rows are the lineage stamp: which pdl_companies Lance version (and its
-- live row count) this sidecar was projected from. A consumer/monitor compares source_version to the
-- live pdl_companies version to detect staleness (plan §9).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pdl_normalized_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,    -- 'pdl_normalized_companies'
    dataset_uri     text        NOT NULL,    -- s3://data-sink/active/pdl_normalized_companies/
    source_dataset  text        NOT NULL,    -- s3://data-sink/active/pdl_companies/
    source_version  bigint,                   -- pdl_companies Lance version projected
    source_rows     bigint,                   -- live source count_rows() at scan time
    rows_processed  bigint,                   -- committed sidecar row count
    distinct_ids    bigint,                   -- exact COUNT(DISTINCT pdl_company_id)
    status          text        NOT NULL,     -- 'success' | 'error'
    error           text,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS pdl_normalized_runs_status_idx       ON ops.pdl_normalized_runs (status);
CREATE INDEX IF NOT EXISTS pdl_normalized_runs_completed_at_idx ON ops.pdl_normalized_runs (completed_at DESC);
CREATE INDEX IF NOT EXISTS pdl_normalized_runs_src_version_idx  ON ops.pdl_normalized_runs (source_version DESC);
