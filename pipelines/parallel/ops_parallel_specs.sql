-- Spec registry for the Parallel.ai capability (Directive 24).
-- Mirrored verbatim by OPS_DDL in the gtm-mcp launch tools (apps/gtm_mcp/src/tools/parallel.py)
-- and by every Parallel Modal worker's self-applied preamble. This file is the reviewable
-- source. One row per persisted spec; the spec's output_schema is the Lance column contract
-- for enrichment specs. Idempotent DDL. Additive only (no DROP/TRUNCATE).

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.parallel_specs (
    spec_id       text PRIMARY KEY,              -- e.g. equipment_profile_v1
    workflow      text NOT NULL                  -- which of the three Parallel workflows
                  CHECK (workflow IN ('enrich','deep_research','search')),
    processor     text NOT NULL,                 -- tier name; enrich caps at 'core', deep_research at 'pro'
    output_schema jsonb,                          -- JSON Schema (enrich); = the Lance column contract. NULL for research/search
    objective     text,                          -- deep_research / search prompt
    grain         text,                          -- deep_research only: 'per_entity' | 'topic'
    dataset_uri   text NOT NULL,                 -- s3://data-sink/active/... landing root
    result_key    text NOT NULL DEFAULT 'company_id',
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS parallel_specs_workflow_idx ON ops.parallel_specs (workflow);
