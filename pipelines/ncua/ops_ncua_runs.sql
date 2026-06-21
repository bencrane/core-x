-- ops.ncua_runs — terminal-state ledger for the NCUA credit-union roster → Lance ingest.
-- Mirrored verbatim by pipelines/ncua/ingest.py (OPS_DDL); applied by `--init-state`.
-- scripts/data-factory-catalog.py reads this to surface NCUA as an active ingest source.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.ncua_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,   -- credit_unions
    dataset_uri    text,                   -- s3://data-sink/active/ncua_credit_unions/
    source_file    text,                   -- workbook basename (FederallyInsuredCreditUnions_<YYYYqQ>.xlsx)
    as_of          date,                   -- roster quarter-end
    rows_processed bigint,
    rejected_rows  bigint,
    indexes_built  text[],
    status         text        NOT NULL,   -- success | error
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ncua_runs_dataset_idx     ON ops.ncua_runs (dataset);
CREATE INDEX IF NOT EXISTS ncua_runs_status_idx      ON ops.ncua_runs (status);
CREATE INDEX IF NOT EXISTS ncua_runs_recorded_at_idx ON ops.ncua_runs (recorded_at DESC);
