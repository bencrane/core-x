-- Audit ledger for the winners-map serving worker.
-- Written by pipelines/serving/materialize_winners_map.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- DERIVED / rebuildable: usaspending_winners_map_serving is a read model (overwrite each
-- run) joining the rolling-window prime+subaward winners to geocode_xwalk on addr_hash.
-- The window is a build parameter, not a property of the crosswalk. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.winners_map_serving_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    window_days    integer,                -- rolling action_date window
    rows_written   bigint,                 -- winners (1 / winner_uei × winner_type)
    with_coords    bigint,                 -- winners that joined a geocode_xwalk dot
    coord_rate     numeric,                -- with_coords / rows_written
    write_mode     text,                   -- 'overwrite'
    indices_built  text,                   -- comma list of BTREE+BITMAP columns
    status         text        NOT NULL,   -- 'success' | 'error'
    error_message  text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS winners_map_serving_runs_status_idx
    ON ops.winners_map_serving_runs (status);
CREATE INDEX IF NOT EXISTS winners_map_serving_runs_recorded_at_idx
    ON ops.winners_map_serving_runs (recorded_at DESC);
