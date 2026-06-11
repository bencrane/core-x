-- Audit ledger for the address-keyed geocode crosswalk worker.
-- Written by pipelines/usaspending/geocode_xwalk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- ACCRETIVE model: geocode_xwalk grows monotonically (merge_insert on addr_hash,
-- when_not_matched_insert_all). A known address is never re-geocoded; each run only
-- pays the external Census cost for addresses it has not seen before. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.geocode_xwalk_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed               text        NOT NULL,
    window_days        integer,                -- rolling source window scanned
    worklist_addresses bigint,                 -- distinct addresses in the window
    already_present    bigint,                 -- addr_hash already resident (skipped)
    newly_geocoded     bigint,                 -- addresses sent to Census this run
    matched            bigint,                 -- Census rooftop matches written
    match_rate         numeric,                -- matched / newly_geocoded
    table_rows_after   bigint,                 -- total rows after this write
    write_mode         text,                   -- 'overwrite' | 'merge_insert' | 'noop'
    indices_built      text,                   -- comma list of BTREE columns
    status             text        NOT NULL,   -- 'success' | 'error'
    error_message      text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS geocode_xwalk_runs_status_idx
    ON ops.geocode_xwalk_runs (status);
CREATE INDEX IF NOT EXISTS geocode_xwalk_runs_recorded_at_idx
    ON ops.geocode_xwalk_runs (recorded_at DESC);
