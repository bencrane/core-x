-- Terminal-state table for the Colorado SoS business-entities bulk ingest worker.
-- Written by pipelines/co_sos/entities_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / SBA feeds (ARCHITECTURE.md §5). Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.co_sos_entity_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file    text        NOT NULL,   -- e.g. Business_Entities_in_Colorado_20260531.csv
    snapshot_date  date,                   -- as-of, decoded from the filename date stamp
    dataset_uri    text,                   -- s3://data-sink/active/co_sos/
    rows_processed bigint,
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS co_sos_entity_runs_source_file_idx   ON ops.co_sos_entity_runs (source_file);
CREATE INDEX IF NOT EXISTS co_sos_entity_runs_status_idx        ON ops.co_sos_entity_runs (status);
CREATE INDEX IF NOT EXISTS co_sos_entity_runs_snapshot_date_idx ON ops.co_sos_entity_runs (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS co_sos_entity_runs_recorded_at_idx   ON ops.co_sos_entity_runs (recorded_at DESC);
