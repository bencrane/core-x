-- Canonical DDL for ops.schema_catalog_runs — the schema-catalog pipeline's
-- terminal-state ledger. Verbatim mirror of OPS_DDL in
-- pipelines/catalog/schema_catalog.py (keep the two in sync). Applied idempotently
-- by every run (and skipped cleanly when HQX_DB_URL_POOLED is unset).
--
-- One row per pipeline run: the run summarizes how many target datasets were read,
-- appended (schema changed/absent), and skipped (fingerprint unchanged), plus the
-- rows appended and indices (re)built. The Lance schema_catalog dataset
-- (s3://data-sink/active/schema_catalog/) is the authoritative system of record for
-- the captured schemas — each catalog row already carries catalog_run_id + captured_at;
-- this table is operational convenience only (per the Substrate Split).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.schema_catalog_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_run_id    text        NOT NULL,
    catalog_uri       text        NOT NULL,
    datasets_total    int,
    datasets_appended int,
    datasets_skipped  int,
    rows_appended     bigint,
    indexes_built     text,
    status            text        NOT NULL,
    error             text,
    metrics           jsonb,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS schema_catalog_runs_run_idx      ON ops.schema_catalog_runs (catalog_run_id);
CREATE INDEX IF NOT EXISTS schema_catalog_runs_status_idx   ON ops.schema_catalog_runs (status);
CREATE INDEX IF NOT EXISTS schema_catalog_runs_recorded_idx ON ops.schema_catalog_runs (recorded_at DESC);
