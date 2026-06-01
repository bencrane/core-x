-- Terminal-state ledger for the CA CSLB (Contractors State License Board) ingest.
-- Mirrored verbatim by OPS_DDL in pipelines/ca_cslb/ingest.py (applied by the
-- apply_state_schema function / `modal run ...::init_state`). This file is the
-- reviewable source. One row per phase per run: phase ∈ {ingest}; target ∈
-- {licenses, personnel, workers_comp}. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.cslb_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,    -- 'ingest'
    target         text,                     -- 'licenses' | 'personnel' | 'workers_comp'
    dataset_uri    text,                     -- s3://data-sink/active/cslb_*
    as_of          date,                     -- export/snapshot date (operator-overridable)
    source_file    text,                     -- MasterLicenseData.csv | PersonnelData.csv | WorkerCompData.csv
    landing_key    text,                     -- landing/ca_cslb/<source_file>
    rows_processed bigint,                   -- committed Lance row count
    rejected_rows  bigint,                   -- malformed rows quarantined by store_rejects
    status         text        NOT NULL,     -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cslb_runs_target_idx      ON ops.cslb_runs (target);
CREATE INDEX IF NOT EXISTS cslb_runs_phase_idx       ON ops.cslb_runs (phase);
CREATE INDEX IF NOT EXISTS cslb_runs_status_idx      ON ops.cslb_runs (status);
CREATE INDEX IF NOT EXISTS cslb_runs_recorded_at_idx ON ops.cslb_runs (recorded_at DESC);
