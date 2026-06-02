-- Terminal-state table for the Blitz firmographics materialization worker (Directive 13).
-- Written by pipelines/firmographics_blitz/materialize_blitz.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the ops.*
-- contract used by every other feed (ARCHITECTURE.md §5). Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies it via
-- `modal run pipelines/firmographics_blitz/materialize_blitz.py::init_ops`. Keep the two in sync.
--
-- Source AND control-plane are the SAME hq-x DB: the worker reads ops.task_runs (READ_ONLY) and
-- writes run-state to ops.firmographics_blitz_runs. One run materializes the deduplicated
-- firmographic reference grain into s3://data-sink/active/firmographics_blitz/.
-- ``datasets`` is the per-table row-count map, e.g. {"firmographics_blitz": 133256}.
-- ``rows_source`` is the pre-dedup completed-row count (e.g. 165884); ``rows_total`` is post-dedup.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.firmographics_blitz_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,            -- 'firmographics_blitz'
    source_db      text        NOT NULL,            -- 'hqx:ops.task_runs'
    datasets       jsonb       NOT NULL,            -- {"firmographics_blitz": n}
    rows_total     bigint      NOT NULL DEFAULT 0,  -- post-dedup row count (= distinct domain_norm)
    rows_source    bigint      NOT NULL DEFAULT 0,  -- pre-dedup completed source rows
    status         text        NOT NULL,            -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_feed_idx        ON ops.firmographics_blitz_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_status_idx      ON ops.firmographics_blitz_runs (status);
CREATE INDEX IF NOT EXISTS firmographics_blitz_runs_recorded_at_idx ON ops.firmographics_blitz_runs (recorded_at DESC);
