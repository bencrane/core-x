-- Terminal-state table for the Gold Mirror reconciliation worker. Written by
-- pipelines/resolution/reconcile_entity_profiles.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used across the SAM / usaspending / crosswalk feeds
-- (ARCHITECTURE.md §5). One row per entity_profile_gold rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it via `modal run pipelines/resolution/reconcile_entity_profiles.py::init_ops`.
-- Keep the two in sync. version_before/version_after record the Lance generations
-- spanned by the guarded overwrite (rollback target ← version_before).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.entity_profile_gold_runs (
    id                             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                           text        NOT NULL,   -- 'entity_profile_gold'
    dataset_uri                    text        NOT NULL,   -- s3://data-sink/active/entity_profile_gold/
    rows_written                   bigint,                 -- committed rows == distinct UEI
    distinct_uei                   bigint,                 -- grain check (== rows_written)
    active_entities                bigint,                 -- is_active = true
    entities_with_pocs             bigint,                 -- pocs list present
    entities_with_awards           bigint,                 -- resolved to ≥1 federal award
    fresh_awards_in_window         bigint,                 -- distinct awards in the 90d override feed
    sum_total_active_obligations   numeric,                -- Σ PoP-active obligated (backlog headline)
    sum_total_lifetime_obligations numeric,                -- Σ lifetime obligated
    indices_built                  text[],                 -- scalar indices verified post-build
    version_before                 bigint,                 -- last-good Lance version (rollback target)
    version_after                  bigint,                 -- published Lance version
    status                         text        NOT NULL,   -- 'success' | 'error'
    error                          text,
    started_at                     timestamptz,
    completed_at                   timestamptz,
    recorded_at                    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS entity_profile_gold_runs_feed_idx        ON ops.entity_profile_gold_runs (feed);
CREATE INDEX IF NOT EXISTS entity_profile_gold_runs_status_idx      ON ops.entity_profile_gold_runs (status);
CREATE INDEX IF NOT EXISTS entity_profile_gold_runs_recorded_at_idx ON ops.entity_profile_gold_runs (recorded_at DESC);
