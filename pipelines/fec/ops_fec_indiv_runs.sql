-- Terminal-state table for the FEC Individual Contributions bulk ingest worker.
-- Written by pipelines/fec/indiv_contributions.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / SBA / SoS feeds (ARCHITECTURE.md §5). Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the _CREATE_TABLE_SQL constant
-- and applies it via `modal run pipelines/fec/indiv_contributions.py::initdb`.
-- Keep the two in sync.
--
-- One row per (cycle_year) ingest of the unified dataset
-- s3://data-sink/active/fec_individual_contributions/. The 24 even-year cycles
-- (1980-2026) each append idempotently (delete WHERE cycle_year=N, then append).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.fec_indiv_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,   -- 'fec_indiv'
    dataset_uri     text        NOT NULL,   -- s3://data-sink/active/fec_individual_contributions/
    cycle_year      smallint    NOT NULL,   -- election cycle (even year, 1980-2026)
    source_file     text        NOT NULL,   -- 'indiv{YY}.zip'
    landing_key     text,                   -- R2 landing key ingested (.txt.zst)
    rows_processed  bigint,
    status          text        NOT NULL,   -- 'success' | 'error'
    error           text,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fec_indiv_runs_feed_idx        ON ops.fec_indiv_runs (feed);
CREATE INDEX IF NOT EXISTS fec_indiv_runs_status_idx      ON ops.fec_indiv_runs (status);
CREATE INDEX IF NOT EXISTS fec_indiv_runs_cycle_idx       ON ops.fec_indiv_runs (cycle_year);
CREATE INDEX IF NOT EXISTS fec_indiv_runs_recorded_at_idx ON ops.fec_indiv_runs (recorded_at DESC);
