-- ops.blitz_email_finder_runs — per-run terminal-state ledger for the standalone
-- Blitz Email Finder (Directive 28). One row per worker invocation (one Trigger
-- chunk → one Modal worker → one row). Mirrors ops.email_cascade_runs /
-- ops.enrichment_blitz_runs. Lives in the hq-x control-plane DB (HQX_DB_URL_POOLED).
--
-- The compute worker (pipelines/enrichment_blitz/enrich_email_standalone.py) applies
-- this DDL defensively before each terminal write, so it re-applies cleanly.
-- Forward-only. IF NOT EXISTS throughout.
--
-- SINK SPLIT. Per-contact verified outcomes are upserted into the SHARED
-- ops.email_resolutions work-email system-of-record (defined canonically in
-- pipelines/enrichment_email_cascade/ops_email_cascade_runs.sql + enrich_email_cascade.py)
-- so ONE downstream materializer rolls both this pipeline's and the cascade's verified
-- emails into Lance with no special-casing. The email_resolutions block below is an
-- IF-NOT-EXISTS mirror (byte-identical to the cascade) for self-bootstrap robustness;
-- this file OWNS only blitz_email_finder_runs.

CREATE SCHEMA IF NOT EXISTS ops;

-- ── Shared work-email system-of-record (canonical owner: enrich_email_cascade.py) ──
CREATE TABLE IF NOT EXISTS ops.email_resolutions (
    contact_id          text        PRIMARY KEY,
    email               text,
    verification_status text        NOT NULL,        -- 'verified' | 'risky' | 'unresolved'
    source_vendor       text,                        -- 'blitz' for this pipeline; 'icypeas'/'leadmagic' for the cascade
    source_tier         int,
    mv_resultcode       int,                          -- MillionVerifier resultcode (sole arbiter)
    mv_result           text,
    mv_quality          text,
    mv_subresult        text,
    certainty           text,                         -- NULL for Blitz (no certainty score)
    company_domain      text,
    person_linkedin_url text,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_resolutions_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
CREATE INDEX IF NOT EXISTS email_resolutions_status_idx ON ops.email_resolutions (verification_status);
CREATE INDEX IF NOT EXISTS email_resolutions_domain_idx ON ops.email_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS email_resolutions_email_idx  ON ops.email_resolutions (email);

-- ── Dedicated per-run ledger (owned by this pipeline) ──────────────────────────
CREATE TABLE IF NOT EXISTS ops.blitz_email_finder_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,              -- 'blitz_email_finder'
    batch_label   text,
    run_root      text,                              -- Trigger run id (or manual uuid)
    priority      text        NOT NULL,              -- gateway lane: 'low' | 'normal'
    requested     bigint      NOT NULL DEFAULT 0,    -- chunk size
    skipped       bigint      NOT NULL DEFAULT 0,    -- already-verified (shared SoR) idempotency
    verified      bigint      NOT NULL DEFAULT 0,    -- MV ok
    risky         bigint      NOT NULL DEFAULT 0,    -- MV catch_all/unknown
    unresolved    bigint      NOT NULL DEFAULT 0,    -- Blitz miss, MV bad, or no person_linkedin_url
    failed        bigint      NOT NULL DEFAULT 0,    -- per-contact exception
    gateway_calls bigint      NOT NULL DEFAULT 0,    -- Blitz email calls via the gateway (egress audit)
    mv_calls      bigint      NOT NULL DEFAULT 0,    -- MillionVerifier calls
    status        text        NOT NULL,              -- 'success' | 'error'
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_email_finder_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT blitz_email_finder_runs_priority_chk CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS blitz_email_finder_runs_feed_idx        ON ops.blitz_email_finder_runs (feed);
CREATE INDEX IF NOT EXISTS blitz_email_finder_runs_recorded_at_idx ON ops.blitz_email_finder_runs (recorded_at DESC);
