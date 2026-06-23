-- ops.mv_verify_runs — per-run terminal-state ledger for the standalone MillionVerifier
-- Email Verifier. One row per worker invocation (one Trigger chunk → one Modal worker →
-- one row). Sibling of ops.blitz_email_finder_runs. Lives in the hq-x control-plane DB
-- (HQX_DB_URL_POOLED).
--
-- The compute worker (pipelines/enrichment_mv/verify_mv_standalone.py) applies this DDL
-- defensively before each terminal write, so it re-applies cleanly. Forward-only.
-- IF NOT EXISTS throughout.
--
-- SINK. Per-contact MV verdicts are upserted into ops.email_verifications — the
-- pre-existing-work-email verification system-of-record owned canonically by THIS
-- pipeline. A downstream materializer (materialize_email_verifications.py) mirrors it
-- to native Lance at s3://data-sink/active/email_verifications/.
--
-- WHAT THIS PIPELINE IS. Input is an ALREADY-KNOWN work email (e.g. gtm.contacts.work_email);
-- the only step is MillionVerifier (the sole deliverability arbiter). There is NO email-
-- FINDING step here (that is the Blitz / Icypeas / LeadMagic job) — this verifies emails
-- we already hold. The house rubric on MV resultcode is the single source of truth:
--   1 ok        → verified | 2 catch_all / 3 unknown → risky | 4/5/6 → unresolved.

CREATE SCHEMA IF NOT EXISTS ops;

-- ── Work-email verification system-of-record (canonical owner: this pipeline) ──
CREATE TABLE IF NOT EXISTS ops.email_verifications (
    contact_id          text        PRIMARY KEY,      -- caller key (e.g. gtm.contacts.record_id)
    email               text        NOT NULL,         -- the work email that was verified
    verification_status text        NOT NULL,         -- 'verified' | 'risky' | 'unresolved'
    mv_resultcode       int,                           -- MillionVerifier resultcode (sole arbiter)
    mv_result           text,
    mv_quality          text,
    mv_subresult        text,
    source              text,                           -- provenance of the input email (e.g. 'gtm.contacts')
    company_domain      text,
    mv_raw              jsonb,                          -- every MillionVerifier response, VERBATIM (array)
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_verifications_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
-- Forward-compatible upgrade for an existing instance. The MV response is stored VERBATIM
-- in mv_raw — no interpretation imposed; the derived columns above (verification_status /
-- mv_*) are a convenience projection ON TOP of it, never a replacement.
ALTER TABLE ops.email_verifications ADD COLUMN IF NOT EXISTS mv_raw jsonb;
CREATE INDEX IF NOT EXISTS email_verifications_status_idx ON ops.email_verifications (verification_status);
CREATE INDEX IF NOT EXISTS email_verifications_domain_idx ON ops.email_verifications (company_domain);
CREATE INDEX IF NOT EXISTS email_verifications_email_idx  ON ops.email_verifications (email);

-- ── Dedicated per-run ledger (owned by this pipeline) ──────────────────────────
CREATE TABLE IF NOT EXISTS ops.mv_verify_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,              -- 'mv_verify'
    batch_label   text,
    run_root      text,                              -- Trigger run id (or manual uuid)
    requested     bigint      NOT NULL DEFAULT 0,    -- chunk size
    skipped       bigint      NOT NULL DEFAULT 0,    -- already-verified (SoR) idempotency
    verified      bigint      NOT NULL DEFAULT 0,    -- MV ok
    risky         bigint      NOT NULL DEFAULT 0,    -- MV catch_all/unknown
    unresolved    bigint      NOT NULL DEFAULT 0,    -- MV bad, or no email
    failed        bigint      NOT NULL DEFAULT 0,    -- per-contact exception
    mv_calls      bigint      NOT NULL DEFAULT 0,    -- MillionVerifier calls
    status        text        NOT NULL,              -- 'success' | 'error'
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT mv_verify_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS mv_verify_runs_feed_idx        ON ops.mv_verify_runs (feed);
CREATE INDEX IF NOT EXISTS mv_verify_runs_recorded_at_idx ON ops.mv_verify_runs (recorded_at DESC);
