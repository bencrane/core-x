-- 20260727213903_ops_federal_appropriations_ingest_runs.sql
-- Migration timestamp generated at write-time (L3). IF NOT EXISTS throughout (L3-idempotent).
--
-- Operational ledger for the federal appropriations & budget-authority ingest
-- (pipelines/reference/federal_appropriations_ingest.py). One row per stream per run.
--
-- L4 (canonical): status CHECK is EXACTLY the three values ('running','completed','failed').
-- The throttle/block/partial vocabulary rides a SEPARATE free-text `disposition` column
-- ('ok'|'throttled'|'blocked'|'partial'|'none') — never the CHECK'd status. A wider CHECK
-- would break idempotent re-runs.
--
-- L60: the two upstream sources register in ops.data_source_catalog (ON CONFLICT DO NOTHING).
-- NOTE (divergence surfaced 2026-07-27): ops.data_source_catalog did not previously exist in
-- the HQX (Supabase) database this ledger lives in — it is created here IF NOT EXISTS from the
-- L60 16-column contract. The ops.data_source_catalog_status VIEW is intentionally NOT
-- recreated: no template migration for it exists in this checkout, and downstream/serving
-- wiring is out of scope for this cycle.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.federal_appropriations_ingest_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        uuid        NOT NULL,
    stream        text        NOT NULL,
    resolved_url  text,
    source_bytes  bigint,
    rows_written  bigint,
    datasets      jsonb,
    started_at    timestamptz,
    finished_at   timestamptz,
    status        text        NOT NULL
                              CHECK (status IN ('running', 'completed', 'failed')),
    disposition   text,       -- free-text: 'ok' | 'throttled' | 'blocked' | 'partial' | 'none'
    notes         text,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS federal_appropriations_ingest_runs_run_idx
    ON ops.federal_appropriations_ingest_runs (run_id);
CREATE INDEX IF NOT EXISTS federal_appropriations_ingest_runs_stream_idx
    ON ops.federal_appropriations_ingest_runs (stream);
CREATE INDEX IF NOT EXISTS federal_appropriations_ingest_runs_status_started_idx
    ON ops.federal_appropriations_ingest_runs (status, started_at);

-- ── L60 canonical source catalog (created only if absent; non-destructive) ──────────
CREATE TABLE IF NOT EXISTS ops.data_source_catalog (
    id                          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_slug                 text        NOT NULL UNIQUE,
    display_name                text        NOT NULL,
    strategic_role              text,
    r2_prefix                   text,
    refresh_cadence             text,
    lifecycle_stage             text,
    owner_team                  text,
    is_active                   boolean     NOT NULL DEFAULT true,
    audit_ledger_table          text,
    essentials_mv_name          text,
    source_url                  text,
    notes                       text,
    bridge_source_name_patterns text,
    audience_mv_name_patterns   text,
    created_at                  timestamptz NOT NULL DEFAULT now()
);

INSERT INTO ops.data_source_catalog
    (source_slug, display_name, strategic_role, r2_prefix, refresh_cadence,
     lifecycle_stage, owner_team, is_active, audit_ledger_table, source_url, notes)
VALUES
    ('omb_public_budget_database',
     'OMB Public Budget Database (Budget Authority + Outlays)',
     'Appropriated budget authority + outlays at account×year grain — the appropriated side of the federal dollar (1976–2031, actuals + vintage estimates).',
     'active/omb_budget_authority/', 'annual (Feb–Apr budget release)', 'active',
     'data-factory', true, 'ops.federal_appropriations_ingest_runs',
     'https://www.whitehouse.gov/omb/information-resources/budget/supplemental-materials/',
     'budauth_fy{YYYY}.xlsx + outlays_fy{YYYY}.xlsx; also lands active/omb_outlays/.'),
    ('usaspending_budgetary_resources',
     'USAspending Budgetary Resources & Account Balances (File A)',
     'Appropriated / obligated / unobligated at federal-account, TAS and agency×FY grain (File A account balances + agency budgetary resources).',
     'active/usaspending_federal_account_balances/', 'quarterly (DABS submissions)', 'active',
     'data-factory', true, 'ops.federal_appropriations_ingest_runs',
     'https://api.usaspending.gov/',
     'download/accounts File A (federal+treasury) + agency budgetary_resources + def_codes + total_budgetary_resources + federal_accounts roster.')
ON CONFLICT (source_slug) DO NOTHING;
