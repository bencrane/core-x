-- Terminal-state table for the GTM minimal-materialization worker (Directive 8).
-- Written by pipelines/gtm/companies_people_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by every other feed (ARCHITECTURE.md §5). Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies
-- it via `modal run pipelines/gtm/companies_people_bulk.py::init_ops`. Keep the two in sync.
--
-- Lives in the HQX control-plane DB (where ca_sos_runs, co_ucc_companions_runs,
-- sam_entity_registration_runs, … all live), NOT in the DEX source DB. The source data
-- (gtm.companies / gtm.people) is read from DEX; run-state is centralized in HQX.
--
-- One run materializes the two entity grains
-- (DEX gtm.companies / gtm.people) into the Gen-3 active sink
-- (s3://data-sink/active/companies/ and s3://data-sink/active/people/).
-- ``datasets`` is the per-table row-count map, e.g. {"companies": 748, "people": 7739}.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.companies_migration_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,   -- 'gtm_companies_people'
    source_db      text        NOT NULL,   -- source identifier ('dex:gtm')
    datasets       jsonb       NOT NULL,   -- {"companies": n, "people": n}
    rows_total     bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS companies_migration_runs_feed_idx        ON ops.companies_migration_runs (feed);
CREATE INDEX IF NOT EXISTS companies_migration_runs_status_idx      ON ops.companies_migration_runs (status);
CREATE INDEX IF NOT EXISTS companies_migration_runs_recorded_at_idx ON ops.companies_migration_runs (recorded_at DESC);
