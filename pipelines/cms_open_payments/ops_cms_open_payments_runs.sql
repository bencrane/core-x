-- Terminal-state ledger for the CMS Open Payments ingest.
-- Mirrored verbatim by OPS_DDL in pipelines/cms_open_payments/ingest.py (applied by the
-- apply_state_schema function / `modal run ...::init_state`). This file is the reviewable
-- source. One row per unit per run: phase ∈ {ingest, refresh_all}; family ∈
-- {general, research, ownership}; payment_year = the CMS program year. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.cms_open_payments_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,    -- 'cms_open_payments'
    phase          text        NOT NULL,    -- 'ingest' (one family-year) | 'refresh_all' (batch summary)
    family         text,                     -- 'general' | 'research' | 'ownership' (NULL on the batch row)
    dataset_uri    text,                     -- s3://data-sink/active/cms_{general_payments,research_payments,ownership}/
    payment_year   smallint,                 -- CMS program year (catalog-authoritative partition key)
    source_file    text,                     -- OP_DTL_{GNRL,RSRCH,OWNRSHP}_PGYR{year}_*.csv
    source_url     text,                     -- resolved metastore downloadURL
    rows_processed bigint,                   -- committed Lance row count (count_rows where payment_year=N)
    rejected_rows  bigint,                   -- malformed rows quarantined by store_rejects
    status         text        NOT NULL,     -- 'success' | 'partial' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cms_op_runs_family_idx      ON ops.cms_open_payments_runs (family);
CREATE INDEX IF NOT EXISTS cms_op_runs_year_idx        ON ops.cms_open_payments_runs (payment_year);
CREATE INDEX IF NOT EXISTS cms_op_runs_phase_idx       ON ops.cms_open_payments_runs (phase);
CREATE INDEX IF NOT EXISTS cms_op_runs_status_idx      ON ops.cms_open_payments_runs (status);
CREATE INDEX IF NOT EXISTS cms_op_runs_recorded_at_idx ON ops.cms_open_payments_runs (recorded_at DESC);
