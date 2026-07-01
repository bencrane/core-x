-- Operational ledger for the DOL SCA ingest (pipelines/dol/ingest.py).
-- Mirrored verbatim by pipelines/dol/ingest.py (OPS_DDL); applied by `--init-state`.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.dol_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,
    dataset_uri    text,
    source_file    text,
    doc_sha256     text,
    rows_processed bigint,
    indexes_built  text[],
    coverage       jsonb,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dol_runs_dataset_idx     ON ops.dol_runs (dataset);
CREATE INDEX IF NOT EXISTS dol_runs_status_idx      ON ops.dol_runs (status);
CREATE INDEX IF NOT EXISTS dol_runs_recorded_at_idx ON ops.dol_runs (recorded_at DESC);
