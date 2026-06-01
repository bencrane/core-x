-- Terminal-state ledger for the NMLS (Nationwide Multistate Licensing System) public
-- reports ingest. Mirrored verbatim by OPS_DDL in pipelines/nmls/ingest.py (applied by
-- the apply_state_schema function / `modal run ...::init_state`). This file is the
-- reviewable source. Two phases, one row each per run:
--   phase = 'acquire' : Playwright/Tier-1 harvest of the public NMLS Business Reports
--                       surface → raw files landed to s3://data-sink/landing/nmls/<as_of>/.
--                       target = 'rosters'; rows_processed = number of files landed.
--   phase = 'ingest'  : DuckDB read_csv/read_xlsx → Lance, per logical target
--                       (e.g. 'mcr_license_activity', 'counts_by_state_agency', ...).
-- Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.nmls_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,    -- 'acquire' | 'ingest'
    target         text,                     -- 'rosters' (acquire) | logical dataset (ingest)
    dataset_uri    text,                     -- s3://data-sink/active/nmls_* (ingest)
    as_of          date,                     -- harvest/snapshot date (operator-overridable)
    source_file    text,                     -- landed member parsed (ingest)
    landing_key    text,                     -- landing/nmls/<as_of>/<file> (ingest) / prefix (acquire)
    rows_processed bigint,                   -- committed Lance row count (ingest) / files landed (acquire)
    rejected_rows  bigint,                   -- malformed rows quarantined by store_rejects (ingest)
    status         text        NOT NULL,     -- 'success' | 'error'
    error          text,
    note           text,                     -- free-text audit (e.g. landed-file manifest summary)
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nmls_runs_phase_idx       ON ops.nmls_runs (phase);
CREATE INDEX IF NOT EXISTS nmls_runs_target_idx      ON ops.nmls_runs (target);
CREATE INDEX IF NOT EXISTS nmls_runs_status_idx      ON ops.nmls_runs (status);
CREATE INDEX IF NOT EXISTS nmls_runs_recorded_at_idx ON ops.nmls_runs (recorded_at DESC);
