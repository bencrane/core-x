-- Terminal-state table for the SBA PPP (FOIA) bulk ingest worker.
-- Written by pipelines/sba_ppp/ppp_loans_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM feeds (ARCHITECTURE.md §5). Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.ppp_loan_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file    text        NOT NULL,   -- e.g. public_150k_plus_240930.csv
    loan_bracket   text,                   -- '150k_plus' | 'up_to_150k'
    rows_processed bigint,
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ppp_loan_runs_source_file_idx ON ops.ppp_loan_runs (source_file);
CREATE INDEX IF NOT EXISTS ppp_loan_runs_status_idx      ON ops.ppp_loan_runs (status);
CREATE INDEX IF NOT EXISTS ppp_loan_runs_recorded_at_idx ON ops.ppp_loan_runs (recorded_at DESC);
