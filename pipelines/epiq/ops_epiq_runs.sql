-- Terminal-state ledger for the Epiq corporate bankruptcy harvest (dm.epiq11.com).
-- Mirrored verbatim by OPS_DDL in pipelines/epiq/ingest.py (applied by the
-- apply_state_schema function / `modal run ...::init_state`). This file is the
-- reviewable source. One row per feed per run: feed ∈ {cases, claims, dockets}.
-- Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.epiq_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,    -- 'cases' | 'claims' | 'dockets'
    run_date        date        NOT NULL,    -- harvest snapshot date (landing partition key)
    dataset_uri     text,                     -- s3://data-sink/active/epiq_*
    project_codes   integer,                  -- cases: universe size; grains: manifest size
    cases_attempted integer,                  -- grains only: project_codes fanned out
    cases_failed    integer,                  -- grains only: per-case fetch failures (partial durability)
    rows_processed  bigint,                   -- committed Lance row count
    status          text        NOT NULL,     -- 'success' | 'error'
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS epiq_runs_feed_idx        ON ops.epiq_runs (feed);
CREATE INDEX IF NOT EXISTS epiq_runs_status_idx      ON ops.epiq_runs (status);
CREATE INDEX IF NOT EXISTS epiq_runs_run_date_idx    ON ops.epiq_runs (run_date DESC);
CREATE INDEX IF NOT EXISTS epiq_runs_recorded_at_idx ON ops.epiq_runs (recorded_at DESC);
