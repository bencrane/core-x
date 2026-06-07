-- Audit ledger for the standalone USAspending API FRESH table worker.
-- Written by pipelines/usaspending/usaspending_api_fresh.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- APPEND-ONLY model: one row per pull. 'backfill' creates the table (write_mode=overwrite,
-- guarded); 'daily' appends (write_mode=append). The table only ever GROWS; a daily pull
-- never overwrites prior data. Overlap re-pulls are intentional (publish lag) and produce
-- harmless duplicate rows reconciled by a downstream mirror, not here. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.usaspending_api_fresh_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,
    run_mode         text        NOT NULL,   -- 'backfill' | 'daily'
    window_start     date        NOT NULL,   -- last_modified_date window lower bound (inclusive)
    window_end       date        NOT NULL,   -- last_modified_date window upper bound (inclusive)
    rows_written     bigint,                 -- rows this pull wrote/appended (NOT deduped)
    columns          integer,                -- verbatim API column count
    table_rows_after bigint,                 -- total rows in the table after this write
    api_calls        integer,                -- bulk_download status-poll count
    write_mode       text,                   -- lance write mode: 'overwrite' (create) | 'append'
    indices_built    text,                   -- comma list of BTREE columns built (create only)
    status           text        NOT NULL,   -- 'success' | 'error'
    error_message    text,                   -- NEVER null when status<>'success' (enforced in worker)
    started_at       timestamptz,
    executed_at      timestamptz NOT NULL DEFAULT now(),
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS usaspending_api_fresh_runs_window_idx
    ON ops.usaspending_api_fresh_runs (window_end DESC);
CREATE INDEX IF NOT EXISTS usaspending_api_fresh_runs_status_idx
    ON ops.usaspending_api_fresh_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_api_fresh_runs_recorded_at_idx
    ON ops.usaspending_api_fresh_runs (recorded_at DESC);
