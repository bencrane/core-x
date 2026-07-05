-- ops.email_resolutions (shared sink) + ops.icypeas_mv_runs (run-state) for the
-- Icypeas+MillionVerifier BULK Work-Email rail (icypeas + millionverifier only).
--
-- Canonical mirror of the OPS_DDL embedded in
-- pipelines/enrichment_icypeas_mv/enrich_icypeas_mv.py (applied idempotently by the worker
-- before each terminal write). Lives in the HQX control-plane DB (HQX_DB_URL_POOLED).
--
-- ops.email_resolutions is the SHARED work-email system-of-record, co-written by the
-- enrichment-email-cascade worker (which owns its canonical DDL) and the standalone Blitz
-- email finder. This rail sets source_vendor='icypeas' + icypeas_raw + mv_raw, and adds the
-- email_domain_norm key; the CREATE/ALTER below are all IF NOT EXISTS so they no-op against
-- an existing instance and never fight another writer for ownership.
--
--   modal run pipelines/enrichment_icypeas_mv/enrich_icypeas_mv.py::init_ops

CREATE SCHEMA IF NOT EXISTS ops;

-- Latest-wins work-email system-of-record, one row per contact_id (upsert). Shared table.
CREATE TABLE IF NOT EXISTS ops.email_resolutions (
    contact_id          text        PRIMARY KEY,
    email               text,
    verification_status text        NOT NULL,    -- verified | risky | unresolved
    source_vendor       text,                    -- icypeas (this rail) | leadmagic | blitz
    source_tier         int,                     -- 1 (icypeas) for this rail
    mv_resultcode       int,                     -- MillionVerifier resultcode (1..6)
    mv_result           text,                    -- ok | catch_all | unknown | error | disposable | invalid
    mv_quality          text,                    -- good | risky | bad
    mv_subresult        text,
    certainty           text,                    -- Icypeas certainty (ultra_sure | very_sure | sure | probable)
    company_domain      text,
    person_linkedin_url text,
    icypeas_raw         jsonb,                    -- drained Icypeas item, VERBATIM (or tagged synthetic)
    leadmagic_raw       jsonb,                    -- (other finder) — untouched by this rail
    blitz_email_raw     jsonb,                    -- (other finder) — untouched by this rail
    mv_raw              jsonb,                    -- array of EVERY MillionVerifier response, VERBATIM
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    batch_label         text,
    resolved_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT email_resolutions_status_chk
        CHECK (verification_status IN ('verified', 'risky', 'unresolved'))
);
-- Additive, idempotent upgrades (a prior writer may already own the base table). Raw payloads
-- are stored VERBATIM per the operator mandate — including no-response (a tagged synthetic /
-- {resultcode:null,error} record), never a silent NULL for a processed contact.
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS icypeas_raw       jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS leadmagic_raw     jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS blitz_email_raw   jsonb;
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS mv_raw            jsonb;
-- email_domain_norm: normalized domain half of the resolved email — the BTREE dedupe/join key
-- the original Directive 21 §8 specified (the as-built cascade dropped it; revived here).
ALTER TABLE ops.email_resolutions ADD COLUMN IF NOT EXISTS email_domain_norm text;
CREATE INDEX IF NOT EXISTS email_resolutions_status_idx      ON ops.email_resolutions (verification_status);
CREATE INDEX IF NOT EXISTS email_resolutions_domain_idx      ON ops.email_resolutions (company_domain);
CREATE INDEX IF NOT EXISTS email_resolutions_email_idx       ON ops.email_resolutions (email);
CREATE INDEX IF NOT EXISTS email_resolutions_email_dnorm_idx ON ops.email_resolutions (email_domain_norm);

-- Per-run terminal state (one row per run_bulk invocation / chunk).
CREATE TABLE IF NOT EXISTS ops.icypeas_mv_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed          text        NOT NULL,
    batch_label   text,
    run_root      text,
    requested     bigint      NOT NULL DEFAULT 0,
    skipped       bigint      NOT NULL DEFAULT 0,   -- already-verified, skipped (idempotency)
    ineligible    bigint      NOT NULL DEFAULT 0,   -- missing name+anchor, never sent to Icypeas
    verified      bigint      NOT NULL DEFAULT 0,
    risky         bigint      NOT NULL DEFAULT 0,
    unresolved    bigint      NOT NULL DEFAULT 0,
    failed        bigint      NOT NULL DEFAULT 0,
    status        text        NOT NULL,            -- success | error
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT icypeas_mv_runs_status_chk CHECK (status IN ('success', 'error'))
);
CREATE INDEX IF NOT EXISTS icypeas_mv_runs_feed_idx        ON ops.icypeas_mv_runs (feed);
CREATE INDEX IF NOT EXISTS icypeas_mv_runs_recorded_at_idx ON ops.icypeas_mv_runs (recorded_at DESC);
