-- Audit ledger for the USAspending award_search RAW API LANDING worker.
-- Written by pipelines/usaspending/usaspending_api_landing.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- AUDIT-ONLY — this is NOT a watermark. The landing window is a fixed rolling
-- [today - WINDOW_DAYS … today]; runs overlap by design and this table merely records
-- what was pulled/landed when. One row per landing run. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.usaspending_award_search_api_landing_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,
    pull_date       date        NOT NULL,   -- == window_end (the dated landing partition)
    window_start    date        NOT NULL,   -- last_modified_date window lower bound (inclusive)
    window_end      date        NOT NULL,   -- last_modified_date window upper bound (inclusive)
    rows_landed     bigint,                 -- verbatim transaction rows landed (NOT deduped)
    columns_landed  integer,                -- source column count (verbatim API width)
    api_calls       integer,                -- bulk_download poll count
    landing_uri     text,                   -- s3://…/usaspending_api_landings/award_search/pull_date=…/
    status          text        NOT NULL,   -- 'success' | 'error'
    error_message   text,                   -- NEVER null when status<>'success' (enforced in worker)
    started_at      timestamptz,
    executed_at     timestamptz NOT NULL DEFAULT now(),  -- terminal-state completion time
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS usaspending_award_search_api_landing_runs_pull_date_idx
    ON ops.usaspending_award_search_api_landing_runs (pull_date DESC);
CREATE INDEX IF NOT EXISTS usaspending_award_search_api_landing_runs_status_idx
    ON ops.usaspending_award_search_api_landing_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_award_search_api_landing_runs_recorded_at_idx
    ON ops.usaspending_award_search_api_landing_runs (recorded_at DESC);
