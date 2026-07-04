-- ops.* control-plane DDL for the STANDALONE LeadMagic firmographic source.
-- Sibling of the Blitz firmographic feed (ops.firmographics_blitz_runs) and the LeadMagic
-- phone finder (ops.leadmagic_phone_finder_runs), but a fully independent vendor path:
-- LeadMagic /company-search → capture (ops) → materialize → Lance SoR
-- s3://data-sink/active/firmographics_leadmagic/.
--
-- Three objects, all in the HQX control-plane DB (HQX_DB_URL_*), idempotent DDL:
--   1. ops.firmographics_leadmagic_capture       — entity-grain capture SoR (verbatim raw + anchors)
--   2. ops.firmographics_leadmagic_finder_runs   — per-run ledger for the CAPTURE worker
--   3. ops.firmographics_leadmagic_runs          — per-run ledger for the MATERIALIZE worker
--
-- CANONICAL COPY. Each worker mirrors the objects it owns as an embedded OPS_DDL constant and
-- applies it via `modal run … ::init_ops`. Keep this file byte-aligned with those constants.

CREATE SCHEMA IF NOT EXISTS ops;

-- ── 1. CAPTURE SoR ────────────────────────────────────────────────────────────────────────
-- One row per caller entity (entity_id PK, latest-wins upsert). The verbatim LeadMagic
-- /company-search response is stored in leadmagic_raw (the source of truth); company_id /
-- b2b_profile_url are a convenience projection extracted at capture time for the skip-set +
-- downstream dedup. This table is ALSO the authoritative (input_domain | input_linkedin_url)
-- → company_id bridge — the materialized Lance grain is company_id, so the per-input mapping
-- lives here, not there. LeadMagic charges only on a HIT, so a 'not_found' row is a free
-- negative cache that prevents re-spend on a settled miss.
CREATE TABLE IF NOT EXISTS ops.firmographics_leadmagic_capture (
    entity_id           text        PRIMARY KEY,          -- caller's stable id for the enriched entity
    input_domain        text,                             -- website/domain sent (raw)
    input_linkedin_url  text,                             -- company_linkedin_url sent (raw) → profile_url
    input_company_name  text,                             -- company name sent (raw)
    company_id          bigint,                           -- LeadMagic companyId (found only)
    b2b_profile_url     text,                             -- response LinkedIn company url (found only)
    company_status      text        NOT NULL,             -- 'found' | 'not_found'
    leadmagic_raw       jsonb,                            -- VERBATIM /company-search response (SoR)
    credits_consumed    integer     NOT NULL DEFAULT 0,   -- LeadMagic charges only on a HIT
    batch_label         text,
    attempts            jsonb       NOT NULL DEFAULT '[]'::jsonb,
    captured_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT firmographics_leadmagic_capture_status_chk
        CHECK (company_status IN ('found', 'not_found'))
);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_capture_status_idx
    ON ops.firmographics_leadmagic_capture (company_status);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_capture_company_id_idx
    ON ops.firmographics_leadmagic_capture (company_id);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_capture_captured_at_idx
    ON ops.firmographics_leadmagic_capture (captured_at DESC);

-- ── 2. CAPTURE run ledger ─────────────────────────────────────────────────────────────────
-- One row per run_leadmagic_company invocation (chunk). Mirrors ops.leadmagic_phone_finder_runs.
CREATE TABLE IF NOT EXISTS ops.firmographics_leadmagic_finder_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,                -- 'firmographics_leadmagic'
    batch_label      text,
    run_root         text,
    priority         text        NOT NULL,
    requested        bigint      NOT NULL DEFAULT 0,
    skipped          bigint      NOT NULL DEFAULT 0,      -- already-captured (found) skip
    found            bigint      NOT NULL DEFAULT 0,
    not_found        bigint      NOT NULL DEFAULT 0,
    failed           bigint      NOT NULL DEFAULT 0,
    api_calls        bigint      NOT NULL DEFAULT 0,      -- LeadMagic /company-search calls made
    credits_consumed bigint      NOT NULL DEFAULT 0,      -- LeadMagic charges only on a HIT
    status           text        NOT NULL,
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT firmographics_leadmagic_finder_runs_status_chk
        CHECK (status   IN ('success', 'error')),
    CONSTRAINT firmographics_leadmagic_finder_runs_priority_chk
        CHECK (priority IN ('low', 'normal'))
);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_finder_runs_feed_idx
    ON ops.firmographics_leadmagic_finder_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_finder_runs_recorded_at_idx
    ON ops.firmographics_leadmagic_finder_runs (recorded_at DESC);

-- ── 3. MATERIALIZE run ledger ─────────────────────────────────────────────────────────────
-- One row per materialization → s3://data-sink/active/firmographics_leadmagic/.
-- Mirrors ops.firmographics_blitz_runs. datasets is the per-table row-count map,
-- e.g. {"firmographics_leadmagic": 41230}. rows_source is the pre-dedup found-capture count;
-- rows_total is post-dedup (= distinct company_id).
CREATE TABLE IF NOT EXISTS ops.firmographics_leadmagic_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,                  -- 'firmographics_leadmagic'
    source_db      text        NOT NULL,                  -- 'hqx:ops.firmographics_leadmagic_capture'
    datasets       jsonb       NOT NULL,                  -- {"firmographics_leadmagic": n}
    rows_total     bigint      NOT NULL DEFAULT 0,        -- post-dedup (= distinct company_id)
    rows_source    bigint      NOT NULL DEFAULT 0,        -- pre-dedup found-capture rows
    status         text        NOT NULL,                  -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_runs_feed_idx
    ON ops.firmographics_leadmagic_runs (feed);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_runs_status_idx
    ON ops.firmographics_leadmagic_runs (status);
CREATE INDEX IF NOT EXISTS firmographics_leadmagic_runs_recorded_at_idx
    ON ops.firmographics_leadmagic_runs (recorded_at DESC);
