-- ops.email_resolutions + ops.email_cascade_runs — control-plane sink + run-state
-- for the Work-Email Waterfall (Directive 21-Final: Icypeas → LeadMagic → MillionVerifier).
--
-- Canonical mirror of the OPS_DDL embedded in
-- pipelines/enrichment_email_cascade/enrich_email_cascade.py (applied idempotently by
-- the worker before each terminal write). Lives in the HQX control-plane DB
-- (HQX_DB_URL_POOLED). A downstream materializer may roll ops.email_resolutions into a
-- Lance work-email dataset on its own cadence, mirroring firmographics-blitz.
--
--   modal run pipelines/enrichment_email_cascade/enrich_email_cascade.py::init_ops

CREATE SCHEMA IF NOT EXISTS ops;

-- Latest-wins work-email system-of-record, one row per contact_id (upsert).
CREATE TABLE IF NOT EXISTS ops.email_resolutions (
    contact_id          text        PRIMARY KEY,
    email               text,
    verification_status text        NOT NULL,    -- verified | risky | unresolved
    source_vendor       text,                    -- icypeas | leadmagic
    source_tier         int,                     -- 1 (icypeas) | 2 (leadmagic)
    mv_resultcode       int,                     -- MillionVerifier resultcode (1..6)
    mv_result           text,                    -- ok | catch_all | unknown | error | disposable | invalid
    mv_quality          text,                    -- good | risky | bad
    mv_subresult        text,
    certainty           text,                    -- Icypeas certainty (ultra_sure | very_sure | sure | probable)
    company_domain      text,
    person_linkedin_url text,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,   -- full per-tier audit trail
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_resolutions_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
CREATE INDEX IF NOT EXISTS email_resolutions_status_idx ON ops.email_resolutions (verification_status);
CREATE INDEX IF NOT EXISTS email_resolutions_domain_idx ON ops.email_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS email_resolutions_email_idx  ON ops.email_resolutions (email);

-- Per-run terminal state (one row per run_cascade invocation / chunk).
CREATE TABLE IF NOT EXISTS ops.email_cascade_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,   -- already-verified, skipped (idempotency)
    verified      bigint      NOT NULL DEFAULT 0,
    risky         bigint      NOT NULL DEFAULT 0,
    unresolved    bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,            -- success | error
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_cascade_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS email_cascade_runs_feed_idx        ON ops.email_cascade_runs (feed);
CREATE INDEX IF NOT EXISTS email_cascade_runs_recorded_at_idx ON ops.email_cascade_runs (recorded_at DESC);
