-- ops.enrichment_blitz_runs — per-run terminal-state ledger for the rate-governed
-- Blitz enrichment plane (Directive 23 §5.3). One row per worker invocation
-- (one Trigger run → one Modal worker → one row). Mirrors
-- ops.firmographics_blitz_runs / ops.companies_migration_runs. Lives in the hq-x
-- control-plane DB (HQX_DB_URL_POOLED), alongside every other ops.*_runs table
-- and the ops.task_runs event log this plane writes its per-entity results to.
--
-- The compute worker (pipelines/enrichment_blitz/enrich_blitz.py) applies this
-- DDL defensively before each terminal write, so it re-applies cleanly.
-- Forward-only. IF NOT EXISTS throughout.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.enrichment_blitz_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,                  -- 'enrichment_blitz'
    workflow       text        NOT NULL,                  -- 'A' | 'B' | 'C'
    batch_label    text,
    priority       text        NOT NULL,                  -- 'high' | 'normal' | 'low'
    run_root       text,                                  -- Trigger run id (or manual uuid)
    requested      bigint      NOT NULL DEFAULT 0,        -- cohort size
    skipped        bigint      NOT NULL DEFAULT 0,        -- warehouse-hit + negative-cache + unnormalizable
    api_calls      bigint      NOT NULL DEFAULT 0,        -- actual gateway calls (5-RPS budget spent)
    succeeded      bigint      NOT NULL DEFAULT 0,        -- resolved (A) / enriched (B,C)
    not_found      bigint      NOT NULL DEFAULT 0,        -- Blitz found:false
    failed         bigint      NOT NULL DEFAULT 0,        -- transport / HTTP error after retries
    status         text        NOT NULL,                  -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT enrichment_blitz_runs_workflow_chk CHECK (workflow IN ('A', 'B', 'C')),
    CONSTRAINT enrichment_blitz_runs_status_chk   CHECK (status   IN ('success', 'error'))
);

CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_feed_idx        ON ops.enrichment_blitz_runs (feed);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_workflow_idx    ON ops.enrichment_blitz_runs (workflow);
CREATE INDEX IF NOT EXISTS enrichment_blitz_runs_recorded_at_idx ON ops.enrichment_blitz_runs (recorded_at DESC);
