-- Audit ledger for the firmographics company-map serving worker.
-- Written by pipelines/serving/materialize_company_map.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- DERIVED / rebuildable: firmographics_company_map_serving is a read model (overwrite each
-- run) joining the firmographics target universe to its SAM UEIs, addresses, and geocode
-- coords. One row per UEI. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.company_map_serving_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed               text        NOT NULL,
    rows_written       bigint,                 -- companies (1 / UEI)
    with_coords        bigint,                 -- companies that joined a geocode_xwalk dot
    coord_rate         numeric,                -- with_coords / rows_written
    has_federal_awards bigint,                 -- companies flagged has_federal_awards
    write_mode         text,                   -- 'overwrite'
    indices_built      text,                   -- comma list of BTREE+BITMAP columns
    status             text        NOT NULL,   -- 'success' | 'error'
    error_message      text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS company_map_serving_runs_status_idx
    ON ops.company_map_serving_runs (status);
CREATE INDEX IF NOT EXISTS company_map_serving_runs_recorded_at_idx
    ON ops.company_map_serving_runs (recorded_at DESC);
