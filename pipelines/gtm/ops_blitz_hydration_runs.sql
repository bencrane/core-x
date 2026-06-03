-- ops.blitz_hydration_runs — per-run terminal-state ledger for the Waterfall ICP
-- contact-hydration pipeline (Directive 20-Final). One row per worker invocation
-- (one Trigger run → one Modal worker → one row). Mirrors ops.enrichment_blitz_runs
-- / ops.firmographics_blitz_runs. Lives in the hq-x control-plane DB
-- (HQX_DB_URL_POOLED), alongside every other ops.*_runs ledger and the
-- ops.campaign_audiences table this pipeline reads its audience definition from.
--
-- The compute worker (pipelines/gtm/blitz_hydration_waterfall.py) applies this DDL
-- defensively before each terminal write, so it re-applies cleanly.
-- Forward-only. IF NOT EXISTS throughout.
--
-- SINK SPLIT. Per-contact RAW payloads land as ZSTD Parquet in R2
-- (dex-raw-landing-zone/blitz_contacts/…, transport only); this table holds the
-- run-level COUNTS + the landing_uri pointer. Promotion of contacts into the Lance
-- system-of-record is a separate directive.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.blitz_hydration_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,                  -- 'blitz_hydration_waterfall'
    campaign_id         text        NOT NULL,
    audience_name       text,                                  -- resolved audience (NULL → campaign latest)
    priority            text        NOT NULL,                  -- gateway lane used ('high'|'normal'|'low')
    run_root            text,                                  -- Trigger run id (or manual uuid)
    companies_in        bigint      NOT NULL DEFAULT 0,        -- domains resolved from the audience
    linkedin_local      bigint      NOT NULL DEFAULT 0,        -- company_linkedin_url from the Lance bridge
    linkedin_api        bigint      NOT NULL DEFAULT 0,        -- resolved via gateway domain→linkedin
    linkedin_unresolved bigint      NOT NULL DEFAULT 0,        -- no URL → skipped (no Waterfall)
    waterfall_calls     bigint      NOT NULL DEFAULT 0,        -- waterfall-icp-keyword calls
    contacts_found      bigint      NOT NULL DEFAULT 0,        -- matched decision-makers
    emails_found        bigint      NOT NULL DEFAULT 0,        -- verified work emails
    phones_found        bigint      NOT NULL DEFAULT 0,        -- direct phones (US)
    gateway_calls       bigint      NOT NULL DEFAULT 0,        -- total gateway calls (5-RPS egress audit)
    email_gated         boolean     NOT NULL DEFAULT false,    -- requested but plan-gated → skipped
    phone_gated         boolean     NOT NULL DEFAULT false,
    landing_uri         text,                                  -- s3://dex-raw-landing-zone/blitz_contacts/…
    status              text        NOT NULL,                  -- 'success' | 'error'
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_hydration_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT blitz_hydration_runs_priority_chk CHECK (priority IN ('high', 'normal', 'low'))
);

CREATE INDEX IF NOT EXISTS blitz_hydration_runs_campaign_idx ON ops.blitz_hydration_runs (campaign_id);
CREATE INDEX IF NOT EXISTS blitz_hydration_runs_status_idx   ON ops.blitz_hydration_runs (status);
CREATE INDEX IF NOT EXISTS blitz_hydration_runs_recorded_idx ON ops.blitz_hydration_runs (recorded_at DESC);
