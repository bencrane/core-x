-- ops.blitz_phone_finder_runs — per-run terminal-state ledger for the standalone
-- Blitz Phone Finder. One row per worker invocation (one Trigger chunk → one Modal
-- worker → one row). Sibling of ops.blitz_email_finder_runs. Lives in the hq-x
-- control-plane DB (HQX_DB_URL_POOLED).
--
-- The compute worker (pipelines/enrichment_blitz/enrich_phone_standalone.py) applies
-- this DDL defensively before each terminal write, so it re-applies cleanly.
-- Forward-only. IF NOT EXISTS throughout.
--
-- SINK. Per-contact phone outcomes are upserted into ops.phone_resolutions — the
-- mobile-phone system-of-record, owned canonically by THIS pipeline (there is no
-- email-style cascade for phones; Blitz is the sole source). A downstream materializer
-- can roll phone_resolutions into Lance the same way email_resolutions is rolled.
--
-- WHY NO MILLIONVERIFIER ANALOG. Blitz phone enrichment returns a direct mobile number
-- (US-only) and is itself terminal — there is no second-vendor deliverability gate the
-- way MillionVerifier arbitrates email. Status is therefore binary: 'found' (Blitz
-- returned a number) vs 'unresolved' (Blitz miss, non-US skip, or no person_linkedin_url).

CREATE SCHEMA IF NOT EXISTS ops;

-- ── Mobile-phone system-of-record (canonical owner: this pipeline) ─────────────
CREATE TABLE IF NOT EXISTS ops.phone_resolutions (
    contact_id          text        PRIMARY KEY,
    phone               text,                            -- E.164 / Blitz-formatted mobile, NULL on miss
    phone_status        text        NOT NULL,            -- 'found' | 'unresolved'
    source_vendor       text,                            -- 'blitz' for this pipeline
    phone_type          text,                            -- Blitz-reported type if present (e.g. 'mobile')
    company_domain      text,
    person_linkedin_url text,
    country_code        text,                            -- US-only gate input, recorded for provenance
    blitz_phone_raw     jsonb,                            -- Blitz /v2/enrichment/phone payload, VERBATIM
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT phone_resolutions_status_chk
        CHECK (phone_status IN ('found', 'unresolved'))
);
-- Forward-compatible upgrades for an existing instance (no-op on fresh create). The
-- Blitz response is stored VERBATIM in blitz_phone_raw — no interpretation imposed; the
-- derived columns above (phone / phone_status / phone_type) are a convenience projection
-- ON TOP of it, never a replacement.
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS blitz_phone_raw jsonb;
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS phone_type      text;
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS country_code    text;
CREATE INDEX IF NOT EXISTS phone_resolutions_status_idx ON ops.phone_resolutions (phone_status);
CREATE INDEX IF NOT EXISTS phone_resolutions_domain_idx ON ops.phone_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS phone_resolutions_phone_idx  ON ops.phone_resolutions (phone);

-- ── Dedicated per-run ledger (owned by this pipeline) ──────────────────────────
CREATE TABLE IF NOT EXISTS ops.blitz_phone_finder_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,              -- 'blitz_phone_finder'
    batch_label   text,
    run_root      text,                              -- Trigger run id (or manual uuid)
    priority      text        NOT NULL,              -- gateway lane: 'low' | 'normal'
    requested     bigint      NOT NULL DEFAULT 0,    -- chunk size
    skipped       bigint      NOT NULL DEFAULT 0,    -- already-found (SoR) idempotency
    found         bigint      NOT NULL DEFAULT 0,    -- Blitz returned a mobile number
    unresolved    bigint      NOT NULL DEFAULT 0,    -- Blitz miss, non-US skip, or no person_linkedin_url
    failed        bigint      NOT NULL DEFAULT 0,    -- per-contact exception
    gateway_calls bigint      NOT NULL DEFAULT 0,    -- Blitz phone calls via the gateway (egress audit)
    status        text        NOT NULL,              -- 'success' | 'error'
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT blitz_phone_finder_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT blitz_phone_finder_runs_priority_chk CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS blitz_phone_finder_runs_feed_idx        ON ops.blitz_phone_finder_runs (feed);
CREATE INDEX IF NOT EXISTS blitz_phone_finder_runs_recorded_at_idx ON ops.blitz_phone_finder_runs (recorded_at DESC);
