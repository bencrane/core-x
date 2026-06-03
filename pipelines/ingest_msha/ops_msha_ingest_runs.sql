-- Terminal-state ledger for the MSHA isolated-materialization ingest (Directive 29).
-- Mirrored verbatim by OPS_DDL in pipelines/ingest_msha/materialize_msha.py (applied by
-- the apply_ops_ddl function / `modal run ...::init_ops`, and defensively before each
-- terminal write). This file is the reviewable source. One row per invocation (full
-- ledger — never upserted), so every materialization run is auditable. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.msha_ingest_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,           -- 'msha'
    source_bucket    text        NOT NULL,           -- 'data-sink'
    source_prefix    text        NOT NULL,           -- 'landing/msha/'
    datasets         jsonb       NOT NULL,           -- {name: {uri, source_archives, spine_rows,
                                                     --         lance_rows, grain_ok, indexes, ...}}
    rows_total       bigint      NOT NULL DEFAULT 0, -- Σ committed Lance rows across datasets
    bytes_downloaded bigint      NOT NULL DEFAULT 0, -- Σ landing-zone .zip bytes pulled from R2
    status           text        NOT NULL,           -- 'success' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS msha_ingest_runs_feed_idx        ON ops.msha_ingest_runs (feed);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_status_idx      ON ops.msha_ingest_runs (status);
CREATE INDEX IF NOT EXISTS msha_ingest_runs_recorded_at_idx ON ops.msha_ingest_runs (recorded_at DESC);
