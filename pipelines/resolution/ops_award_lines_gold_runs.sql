-- Terminal-state table for the per-UEI award-lines Gold Mirror worker. Written by
-- pipelines/resolution/award_lines_gold.py:_record_run via psycopg (HQX_DB_URL_POOLED)
-- on every terminal state, success or failure — mirrors the ops.* contract used across
-- the SAM / usaspending / crosswalk feeds (ARCHITECTURE.md §5). One row per
-- entity_award_lines_gold rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies it
-- via `modal run pipelines/resolution/award_lines_gold.py::init_ops`. Keep the two in sync.
-- version_before/version_after record the Lance generations spanned by the guarded overwrite
-- (rollback target ← version_before).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.entity_award_lines_gold_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text        NOT NULL,   -- 'entity_award_lines_gold'
    dataset_uri              text        NOT NULL,   -- s3://data-sink/active/entity_award_lines_gold/
    rows_written             bigint,                 -- committed rows == distinct UEI (with ≥1 award)
    distinct_uei             bigint,                 -- grain check (== rows_written)
    entities_with_active     bigint,                 -- ≥1 PoP-active prime line item
    entities_with_closed     bigint,                 -- ≥1 PoP-elapsed (past-performance) line item
    sum_active_line_count    bigint,                 -- Σ pre-cap active line items across entities
    sum_closed_line_count    bigint,                 -- Σ pre-cap closed line items across entities
    indices_built            text[],                 -- scalar indices verified post-build (uei_idx)
    version_before           bigint,                 -- last-good Lance version (rollback target)
    version_after            bigint,                 -- published Lance version
    status                   text        NOT NULL,   -- 'success' | 'error'
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS entity_award_lines_gold_runs_feed_idx        ON ops.entity_award_lines_gold_runs (feed);
CREATE INDEX IF NOT EXISTS entity_award_lines_gold_runs_status_idx      ON ops.entity_award_lines_gold_runs (status);
CREATE INDEX IF NOT EXISTS entity_award_lines_gold_runs_recorded_at_idx ON ops.entity_award_lines_gold_runs (recorded_at DESC);
