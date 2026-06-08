-- Audit ledger for the SUBAWARDEE WORK PROFILE (Tier 0) snapshot builder.
-- Written by pipelines/usaspending/subawardee_work_profile.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- SNAPSHOT model (NOT append-accumulating): the per-entity 5-year work profile is a full
-- recompute over a sliding window, landed mode="overwrite" to
-- s3://data-sink/active/subawardee_work_profile/. One ledger row per build records the
-- window width and the row count materialized. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.subawardee_work_profile_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    window_years  integer     NOT NULL,   -- trailing window width in years (default 5)
    rows_written  bigint,                 -- one row per distinct subawardee_uei
    status        text        NOT NULL,   -- 'success' | 'error'
    error_message text,                   -- NEVER null when status<>'success' (enforced in worker)
    started_at    timestamptz,
    executed_at   timestamptz NOT NULL DEFAULT now(),
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS subawardee_work_profile_runs_status_idx
    ON ops.subawardee_work_profile_runs (status);
CREATE INDEX IF NOT EXISTS subawardee_work_profile_runs_recorded_at_idx
    ON ops.subawardee_work_profile_runs (recorded_at DESC);
