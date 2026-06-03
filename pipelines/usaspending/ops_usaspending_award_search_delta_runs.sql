-- Control ledger for the USAspending award_search DAILY DELTA worker.
-- Written by pipelines/usaspending/usaspending_daily_delta.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by usaspending_bulk / contractor_award_summary (ARCHITECTURE.md §5).
--
-- One row per delta run (cold-start catch-up or a steady-state day). This table is
-- also the WATERMARK: the next window starts the day after
--   max(feed_date) WHERE status = 'success'
-- and an empty table (no successful run) selects the cold-start branch. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.usaspending_award_search_delta_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed_date       date        NOT NULL,   -- watermark stamp (== window_end, i.e. "yesterday")
    window_start    date        NOT NULL,   -- last_modified_date window lower bound (inclusive)
    window_end      date        NOT NULL,   -- last_modified_date window upper bound (inclusive)
    run_mode        text        NOT NULL,   -- 'cold_start' (bulk_download/awards) | 'steady_state' (spending_by_award)
    rows_upserted   bigint,                 -- award-grain rows merge_insert'd into award_search
    api_calls       integer,                -- spending_by_award page calls (steady) / bulk poll count (cold)
    raw_landing_uri text,                   -- s3://dex-raw-landing-zone/usaspending/award_search/api-delta/date=.../...
    dataset_uri     text,                   -- merge target (award_search Lance)
    status          text        NOT NULL,   -- 'success' | 'error' | 'no_data' | 'skipped'
    error_message   text,
    started_at      timestamptz,
    executed_at     timestamptz NOT NULL DEFAULT now(),  -- terminal-state completion time
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS usaspending_award_search_delta_runs_feed_date_idx
    ON ops.usaspending_award_search_delta_runs (feed_date DESC);
CREATE INDEX IF NOT EXISTS usaspending_award_search_delta_runs_status_idx
    ON ops.usaspending_award_search_delta_runs (status);
-- Watermark hot path: latest successful run.
CREATE INDEX IF NOT EXISTS usaspending_award_search_delta_runs_success_feed_idx
    ON ops.usaspending_award_search_delta_runs (feed_date DESC) WHERE status = 'success';
CREATE INDEX IF NOT EXISTS usaspending_award_search_delta_runs_recorded_at_idx
    ON ops.usaspending_award_search_delta_runs (recorded_at DESC);
