-- ops.nppes_taxonomy_ref_runs — operational ledger for the NUCC taxonomy reference ingest.
-- Reviewable mirror of OPS_DDL in pipelines/nppes/taxonomy_ref.py (applied by init_state).
-- One row per ingest of nppes_taxonomy_ref (the code → specialty-name crosswalk). Best-effort
-- write — an audit-write failure never masks a good build.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.nppes_taxonomy_ref_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,            -- 'nppes_taxonomy_ref'
    source_url          text,                            -- NUCC CSV URL (version encoded in filename)
    nucc_version        text,                            -- e.g. '25.1'
    rows                bigint,                          -- committed rows (expect ~883 @ v25.1)
    dataset_uri         text,                            -- s3://data-sink/active/nppes_taxonomy_ref/
    indices_built       text,                            -- e.g. 'BTREE:taxonomy_code,BITMAP:grouping,BITMAP:section'
    coverage_pct        double precision,                -- % of live nppes_provider_taxonomy rows that resolve to a name
    unmatched_codes     bigint,                          -- distinct live codes absent from this edition (retired codes)
    gate                jsonb,                           -- per-gate verdicts
    status              text        NOT NULL,            -- 'success' | 'error'
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nppes_taxonomy_ref_runs_recorded_idx ON ops.nppes_taxonomy_ref_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS nppes_taxonomy_ref_runs_status_idx   ON ops.nppes_taxonomy_ref_runs (status);
