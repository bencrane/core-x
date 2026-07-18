-- Terminal-state run ledger for the Parallel.ai capability (Directive 24).
-- Mirrors ops_cslb_runs.sql / ops.exa_webset_runs. Mirrored verbatim by OPS_DDL in every
-- Parallel Modal worker (self-applied before each terminal write) so the table bootstraps
-- on the first real run. One row per dispatched run (enrich group, deep-research run/topic,
-- or search). Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.parallel_runs (
    id                 bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    spec_id            text NOT NULL,
    workflow           text NOT NULL
                       CHECK (workflow IN ('enrich','deep_research','search')),
    run_kind           text NOT NULL
                       CHECK (run_kind IN ('test','full','live')),
    group_id           text,                       -- Parallel task_group_id (enrich) / run_id (research/search)
    audience_id        uuid,
    idempotency_key    text,
    requested          bigint,                     -- entities/inputs dispatched
    skipped_no_domain  bigint,                     -- companies sent name-only (null domain)
    completed          bigint,
    failed             bigint,
    failed_company_ids jsonb NOT NULL DEFAULT '[]'::jsonb,
    processor          text,
    cost_estimate      numeric,
    cost_cap           numeric,
    dataset_uri        text,
    status             text NOT NULL,              -- success | partial | rejected | failed
    error              text,
    started_at         timestamptz,
    completed_at       timestamptz,
    recorded_at        timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS parallel_runs_spec_idx     ON ops.parallel_runs (spec_id);
CREATE INDEX IF NOT EXISTS parallel_runs_workflow_idx ON ops.parallel_runs (workflow);
CREATE INDEX IF NOT EXISTS parallel_runs_status_idx   ON ops.parallel_runs (status);
CREATE INDEX IF NOT EXISTS parallel_runs_group_idx    ON ops.parallel_runs (group_id);
CREATE INDEX IF NOT EXISTS parallel_runs_recorded_idx ON ops.parallel_runs (recorded_at DESC);
