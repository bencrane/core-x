-- Operational ledger for the OMB Public Apportionment (SF-132) R2/Lance ingest, plus the
-- L60 canonical data-source catalog bootstrap + registration.
--
-- Directive: docs/plans/2026-07-27-OMB_APPORTIONMENT_INGEST_DIRECTIVE.md.
-- Applied inline by pipelines/reference/omb_apportionment_ingest.py (_ensure_ledger_and_catalog)
-- on every run; this file is the committed artifact of record. All statements are idempotent
-- (IF NOT EXISTS / ON CONFLICT DO NOTHING).
--
-- Ledger status obeys the canonical L4 enum ('running','completed','failed'); throttle/block/
-- partial states ride the free-text `disposition` column, NEVER `status`.

CREATE SCHEMA IF NOT EXISTS ops;

-- ── Layer 0: run ledger ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS ops.omb_apportionment_ingest_runs (
    run_id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    stream            text        NOT NULL,          -- index | files | schedule | footnotes | all
    index_link_count  integer,
    files_fetched     integer,
    files_failed      integer,
    rows_written      bigint,
    datasets          jsonb,                          -- {dataset: rows_written}
    started_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz,
    status            text        NOT NULL CHECK (status IN ('running','completed','failed')),
    disposition       text,                           -- free-text: 'throttled' | 'partial' | NULL
    notes             text
);
CREATE INDEX IF NOT EXISTS idx_omb_apportionment_ingest_runs_status_started
    ON ops.omb_apportionment_ingest_runs (status, started_at);

-- ── Layer 1: L60 canonical catalog (bootstrap in the Gen-3/HQX plane) ────────────────
-- The canonical 16-col ops.data_source_catalog originates in the data-engine-x Supabase
-- plane; HQX (the core-x Gen-3 control plane) has no catalog yet. Bootstrap the same schema
-- here so this and future core-x ingests register canonically. The cross-plane status VIEW
-- (audience/bridge/r2-snapshot joins) is that plane's dashboard concern and is intentionally
-- NOT ported — none of its dependency tables exist in HQX.
CREATE TABLE IF NOT EXISTS ops.data_source_catalog (
  source_slug         TEXT PRIMARY KEY,
  display_name        TEXT NOT NULL,
  strategic_role      TEXT NOT NULL,
  r2_prefix           TEXT NOT NULL,
  refresh_cadence     TEXT NOT NULL CHECK (refresh_cadence IN
                        ('one-shot','daily','weekly','monthly','quarterly','biennial','annual','on-demand')),
  lifecycle_stage     TEXT NOT NULL CHECK (lifecycle_stage IN
                        ('discovery','r2_only','rw_source_wired','essentials_hydrated',
                         'bridge_layer','audience_layer','streaming_refresh')),
  audit_ledger_table  TEXT,
  essentials_mv_name  TEXT,
  source_url          TEXT,
  notes               TEXT,
  owner_team          TEXT NOT NULL DEFAULT 'data-factory',
  is_active           BOOLEAN NOT NULL DEFAULT TRUE,
  bridge_source_name_patterns TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  audience_mv_name_patterns   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (source_slug ~ '^[a-z0-9_]+$'),
  CHECK (display_name <> ''),
  CHECK (strategic_role <> ''),
  CHECK (r2_prefix <> '')
);
CREATE INDEX IF NOT EXISTS idx_data_source_catalog_lifecycle_stage
  ON ops.data_source_catalog (lifecycle_stage);
CREATE INDEX IF NOT EXISTS idx_data_source_catalog_is_active
  ON ops.data_source_catalog (is_active) WHERE is_active = TRUE;

INSERT INTO ops.data_source_catalog
  (source_slug, display_name, strategic_role, r2_prefix, refresh_cadence, lifecycle_stage,
   audit_ledger_table, essentials_mv_name, source_url, notes,
   bridge_source_name_patterns, audience_mv_name_patterns, is_active)
VALUES
  ('omb_apportionment',
   'OMB Public Apportionment (SF-132)',
   'The missing middle step between appropriation and obligation — the earliest public, line-item release of budget authority to agencies; the only feed carrying the FundsProvidedBy public-law attribution (OBBA / P.L. candidate).',
   'active/omb_apportionment_files/', 'quarterly', 'r2_only',
   'ops.omb_apportionment_ingest_runs', NULL,
   'https://apportionment-public.max.gov/',
   'Three Lance datasets: active/omb_apportionment_{files,lines,footnotes}/. Line grain is SF-132 schedule lines (budgetary_resource vs application_of_resource halves, equal by construction). Catalog bootstrapped in HQX (Gen-3 plane had no catalog yet).',
   ARRAY['source_omb_apportionment_%'], ARRAY['mv_audience_omb_apportionment_%'], TRUE)
ON CONFLICT (source_slug) DO NOTHING;
