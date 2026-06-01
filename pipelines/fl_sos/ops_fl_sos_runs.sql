-- Terminal-state ledger for the FL SoS (Sunbiz) ingest. Mirrored verbatim by
-- OPS_DDL in pipelines/fl_sos/sunbiz.py (applied by the apply_state_schema function).
-- One row per phase per run: phase ∈ {explode, ingest}; target ∈ {master, events}.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.fl_sos_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,
    target         text,
    dataset_uri    text,
    as_of          date,
    source_zip     text,
    landing_key    text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fl_sos_runs_target_idx      ON ops.fl_sos_runs (target);
CREATE INDEX IF NOT EXISTS fl_sos_runs_phase_idx       ON ops.fl_sos_runs (phase);
CREATE INDEX IF NOT EXISTS fl_sos_runs_status_idx      ON ops.fl_sos_runs (status);
CREATE INDEX IF NOT EXISTS fl_sos_runs_recorded_at_idx ON ops.fl_sos_runs (recorded_at DESC);
