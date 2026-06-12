-- Audit ledger for the award-grain map serving worker.
-- Written by pipelines/serving/materialize_awards_map.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- DERIVED / rebuildable: usaspending_awards_map_serving is a read model (overwrite each
-- run): ONE ROW PER POSITIVE-DOLLAR AWARD ACTION from the rolling fresh feeds (prime txn
-- + subaward), deduped on the transaction key, joined to geocode_xwalk on addr_hash.
-- De-obligations (amount < 0) and $0 admin mods are EXCLUDED at build — ">$X won"
-- semantics must never match a de-obligation. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.awards_map_serving_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    window_days    integer,                -- rolling action_date window
    rows_written   bigint,                 -- award actions (post amount>0 + dedupe)
    prime_rows     bigint,                 -- prime_recipient actions
    sub_rows       bigint,                 -- subawardee actions
    with_coords    bigint,                 -- actions that joined a geocode_xwalk dot
    coord_rate     numeric,                -- with_coords / rows_written
    write_mode     text,                   -- 'overwrite'
    indices_built  text,                   -- comma list of BTREE+BITMAP columns
    status         text        NOT NULL,   -- 'success' | 'error'
    error_message  text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS awards_map_serving_runs_status_idx
    ON ops.awards_map_serving_runs (status);
CREATE INDEX IF NOT EXISTS awards_map_serving_runs_recorded_at_idx
    ON ops.awards_map_serving_runs (recorded_at DESC);
