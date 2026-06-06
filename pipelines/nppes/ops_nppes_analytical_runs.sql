-- Terminal-state ledger for the NPPES derived analytical serving layer.
-- Mirrored verbatim by OPS_DDL in pipelines/nppes/materialize_analytical.py (applied by the
-- apply_state_schema function / `modal run ...::init_state`). This file is the reviewable
-- source. One row per materialize run (full ledger — never upserted), so the build history
-- of every monthly snapshot is auditable. Idempotent DDL.
--
-- Separate from ops.nppes_runs (the raw-ingest ledger): the analytical layer is a NEW Modal
-- app with its own blast radius (reads the raw SoR read-only; writes only the three derived
-- prefixes). A failure here can never corrupt the raw capture, and the ledgers stay disjoint.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.nppes_analytical_runs (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                    text        NOT NULL,    -- 'nppes_analytical'
    snapshot_month          text        NOT NULL,    -- 'YYYY-MM' partition/vintage key
    source_dataset_uri      text,                    -- s3://data-sink/active/nppes/snapshot=YYYY-MM/ (raw SoR, read-only)
    source_version          bigint,                  -- raw Lance dataset version read
    provider_rows           bigint,                  -- committed nppes_provider rows (expect 9,551,447 @ 2026-05)
    taxonomy_rows           bigint,                  -- committed nppes_provider_taxonomy rows (expect 11,952,809)
    identifier_rows         bigint,                  -- committed nppes_provider_identifier rows (expect 2,759,800)
    date_parse_failures     bigint,                  -- total %m/%d/%Y parse failures across all 5 date columns (gate G8)
    dirty_state_nulled      bigint,                  -- practice_state non-null source → NULL after clean_state (gate G9 context)
    provider_dataset_uri    text,                    -- s3://data-sink/active/nppes_provider/snapshot=YYYY-MM/
    taxonomy_dataset_uri    text,                    -- s3://data-sink/active/nppes_provider_taxonomy/snapshot=YYYY-MM/
    identifier_dataset_uri  text,                    -- s3://data-sink/active/nppes_provider_identifier/snapshot=YYYY-MM/
    indices_built           text,                    -- csv of name:TYPE:col across all three datasets
    datasets_published      text,                    -- csv of prefixes published this run (torn-state signal with status='partial')
    g3_cold_ms              double precision,        -- specialty BITMAP count, cold R2 (recorded for trend; NEVER gated)
    g6_cold_ms              double precision,        -- specialty×geo join, cold R2 (recorded for trend; NEVER gated)
    gate                    jsonb,                   -- full per-assertion §8 gate result (G1..G12)
    status                  text        NOT NULL,    -- 'success' | 'partial' | 'error'
    error                   text,
    started_at              timestamptz,
    completed_at            timestamptz,
    recorded_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS nppes_analytical_runs_month_idx    ON ops.nppes_analytical_runs (snapshot_month);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_feed_idx     ON ops.nppes_analytical_runs (feed);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_status_idx   ON ops.nppes_analytical_runs (status);
CREATE INDEX IF NOT EXISTS nppes_analytical_runs_recorded_idx ON ops.nppes_analytical_runs (recorded_at DESC);
