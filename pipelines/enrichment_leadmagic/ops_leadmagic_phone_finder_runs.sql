-- ops.leadmagic_phone_finder_runs — per-run state ledger for the LeadMagic mobile finder
-- (pipelines/enrichment_leadmagic/find_phone_leadmagic.py). Sibling of
-- ops.blitz_phone_finder_runs; one row per Modal run_leadmagic_phone invocation (chunk).
--
-- The LeadMagic finder shares the Blitz finder's mobile system-of-record,
-- ops.phone_resolutions (contact_id PK, latest-wins upsert), distinguished by
-- source_vendor='leadmagic'. The verbatim LeadMagic /mobile-finder response lands in the
-- leadmagic_raw column (added idempotently below); phone / phone_status / phone_type are a
-- convenience projection on top. This is the authoritative DDL mirror of the worker's
-- embedded OPS_DDL — keep them byte-aligned.

CREATE SCHEMA IF NOT EXISTS ops;

-- Shared mobile SoR (created by whichever finder runs first; idempotent).
CREATE TABLE IF NOT EXISTS ops.phone_resolutions (
    contact_id          text        PRIMARY KEY,
    phone               text,
    phone_status        text        NOT NULL,
    source_vendor       text,
    phone_type          text,
    company_domain      text,
    person_linkedin_url text,
    country_code        text,
    blitz_phone_raw     jsonb,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT phone_resolutions_status_chk CHECK (phone_status IN ('found', 'unresolved'))
);
-- LeadMagic verbatim payload column (Blitz uses blitz_phone_raw; LeadMagic uses leadmagic_raw).
ALTER TABLE ops.phone_resolutions ADD COLUMN IF NOT EXISTS leadmagic_raw jsonb;

CREATE TABLE IF NOT EXISTS ops.leadmagic_phone_finder_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,                  -- 'leadmagic_phone_finder'
    batch_label      text,
    run_root         text,
    priority         text        NOT NULL,
    requested        bigint      NOT NULL DEFAULT 0,
    skipped          bigint      NOT NULL DEFAULT 0,        -- already-found (cross-vendor) skip
    found            bigint      NOT NULL DEFAULT 0,
    unresolved       bigint      NOT NULL DEFAULT 0,
    failed           bigint      NOT NULL DEFAULT 0,
    api_calls        bigint      NOT NULL DEFAULT 0,        -- LeadMagic /mobile-finder calls made
    credits_consumed bigint      NOT NULL DEFAULT 0,        -- LeadMagic charges only on a HIT
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT leadmagic_phone_finder_runs_status_chk   CHECK (status   IN ('success', 'error')),
    CONSTRAINT leadmagic_phone_finder_runs_priority_chk CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS leadmagic_phone_finder_runs_feed_idx        ON ops.leadmagic_phone_finder_runs (feed);
CREATE INDEX IF NOT EXISTS leadmagic_phone_finder_runs_recorded_at_idx ON ops.leadmagic_phone_finder_runs (recorded_at DESC);
