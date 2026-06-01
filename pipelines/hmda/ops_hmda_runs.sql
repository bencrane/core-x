-- Terminal-state ledger for the HMDA LAR + Reporter Panel ingest. Mirrored verbatim by
-- OPS_DDL in pipelines/hmda/hmda_bulk.py (applied by the apply_state_schema function).
-- One row per (dataset, data_year) ingest attempt: dataset ∈ {lar, panels}.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.hmda_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset        text        NOT NULL,            -- lar | panels
    data_year      int,
    source_product text,                            -- snapshot_lar | combined_mlar | historic_* | ts_2024[_fallback]
    schema_era     text,                            -- modern | mlar | legacy | ts
    source_url     text,
    expected_bytes bigint,                           -- embedded source-map Content-Length
    actual_bytes   bigint,                           -- live downloaded byte count
    size_verified  boolean,                          -- actual == expected
    dataset_uri    text,
    rows_processed bigint,
    rejected_rows  bigint,
    status         text        NOT NULL,            -- success | error
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS hmda_runs_dataset_idx     ON ops.hmda_runs (dataset);
CREATE INDEX IF NOT EXISTS hmda_runs_year_idx        ON ops.hmda_runs (data_year);
CREATE INDEX IF NOT EXISTS hmda_runs_status_idx      ON ops.hmda_runs (status);
CREATE INDEX IF NOT EXISTS hmda_runs_recorded_at_idx ON ops.hmda_runs (recorded_at DESC);
