-- Terminal-state table for the USAspending FFATA Executive Compensation worker.
-- Written by pipelines/usaspending/ffata_exec_comp.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / crosswalk / usaspending feeds (ARCHITECTURE.md
-- §5). One row per rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it via `modal run pipelines/usaspending/ffata_exec_comp.py::init_ops`.
-- Keep the two in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.ffata_exec_comp_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,   -- 'ffata_exec_comp'
    dataset_uri     text        NOT NULL,   -- s3://data-sink/active/ffata_exec_comp/
    rows_written    bigint,                 -- officer rows (1 per uei × populated rank)
    distinct_uei    bigint,                 -- recipient UEIs with ≥1 reported officer
    officer_rows    bigint,                 -- == rows_written (named officers retained)
    status          text        NOT NULL,   -- 'success' | 'error'
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ffata_exec_comp_runs_feed_idx
    ON ops.ffata_exec_comp_runs (feed);
CREATE INDEX IF NOT EXISTS ffata_exec_comp_runs_status_idx
    ON ops.ffata_exec_comp_runs (status);
CREATE INDEX IF NOT EXISTS ffata_exec_comp_runs_recorded_at_idx
    ON ops.ffata_exec_comp_runs (recorded_at DESC);
