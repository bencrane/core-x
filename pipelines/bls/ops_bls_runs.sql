-- Operational ledger for the BLS ingest (pipelines/bls/ingest.py).
-- Mirrored verbatim by pipelines/bls/ingest.py (OPS_DDL); applied by `--init-state`.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.bls_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_file    text,
    release        text,
    rows_processed bigint,
    rejected_rows  bigint,
    indexes_built  text[],
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS bls_runs_dataset_idx     ON ops.bls_runs (dataset);
CREATE INDEX IF NOT EXISTS bls_runs_status_idx      ON ops.bls_runs (status);
CREATE INDEX IF NOT EXISTS bls_runs_recorded_at_idx ON ops.bls_runs (recorded_at DESC);
