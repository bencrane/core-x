-- ops.fdic_runs — terminal-state ledger for the FDIC BankFind landing → Lance ingest.
-- Mirrored verbatim by pipelines/fdic/ingest.py (OPS_DDL); applied by `--init-state`.
-- One row per member per run. scripts/data-factory-catalog.py reads this to surface
-- FDIC as an active ingest source.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.fdic_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,   -- institutions | financial | locations | sod | structure_changes | failures
    dataset_uri    text,                   -- s3://data-sink/active/fdic_<member>/
    source_file    text,                   -- landing CSV basename
    as_of          date,                   -- snapshot date (BankFind export date)
    rows_processed bigint,
    rejected_rows  bigint,                 -- ragged rows quarantined by store_rejects
    indexes_built  text[],                 -- e.g. {BTREE:cert, BITMAP:bkclass}
    status         text        NOT NULL,   -- success | error
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fdic_runs_dataset_idx     ON ops.fdic_runs (dataset);
CREATE INDEX IF NOT EXISTS fdic_runs_status_idx      ON ops.fdic_runs (status);
CREATE INDEX IF NOT EXISTS fdic_runs_recorded_at_idx ON ops.fdic_runs (recorded_at DESC);
