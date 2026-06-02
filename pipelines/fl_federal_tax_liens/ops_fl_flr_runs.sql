-- Terminal-state ledger for the Florida Federal Lien Registrations (FLR) ingest.
-- Mirrored verbatim by OPS_DDL in pipelines/fl_federal_tax_liens/ingest.py (applied by the
-- init_db function / `modal run ...::setup`, and re-asserted idempotently on every run by
-- _record_run). This file is the reviewable source. One row per phase per run:
-- phase ∈ {ingest, reindex}. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.fl_flr_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase            text        NOT NULL,     -- 'ingest' | 'reindex'
    dataset_uri      text,                     -- s3://data-sink/active/fl_federal_tax_liens/
    as_of            date,                     -- quarterly export fulfillment date (operator-overridable)
    source_zips      jsonb,                    -- {filing,debtor,secured: extracted byte sizes}
    filing_rows      bigint,                   -- parsed filings (FLRF)
    secured_rows     bigint,                   -- parsed secured parties (FLRS)
    dropped_sentinel bigint,                   -- corporate debtor rows dropped on doc 26FLR0000999
    rows_processed   bigint,                   -- committed Lance rows (debtor grain)
    indexes          jsonb,                    -- ["normalized_legal_name","zip5","doc_number"]
    index_mode       text,                     -- 'direct-r2' | 'local-roundtrip'
    status           text        NOT NULL,     -- 'success' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fl_flr_runs_phase_idx       ON ops.fl_flr_runs (phase);
CREATE INDEX IF NOT EXISTS fl_flr_runs_status_idx      ON ops.fl_flr_runs (status);
CREATE INDEX IF NOT EXISTS fl_flr_runs_recorded_at_idx ON ops.fl_flr_runs (recorded_at DESC);
