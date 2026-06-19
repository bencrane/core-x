-- Per-run roll-up for the SAM.gov 90-day attachment GTM SCOPE RESOLVER.
-- Written by pipelines/sam_gov/sam_attachment_gtm_scope_90day.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, one row per run_id.
--
-- The resolver verdicts every downloaded attachment (sam_attachment_files) as in_scope /
-- out_of_scope for the "Strained Middle" mid-market GTM cohort, by bridging resource_id -> the prime
-- award (sam_opps_attachment_manifest_90day_winners.contract_award_unique_key / award_keys) and scoring
-- the award against usaspending_api_fresh/contract_prime_txn on three rules:
--   A  current_total_value_of_award in [$500k, $15M]
--   B  6-digit award NAICS in the labor/capital-intensive target set (construction / specialty trade /
--      heavy-civil mobilization / environmental remediation)
--   C  frequency cap: drop entities with > 30 in-band awards over the 90-day window (de-contaminates the
--      high-frequency distributors/OEMs that pollute a raw dollar ranking)
-- Output -> active/sam_attachment_gtm_scope/ (Lance v2.1, BTREE resource_id, BITMAP gtm_scope/scope_reason),
-- consumed by sam_attachment_extract_90day.py Phase 1 as a terminal `skipped_out_of_scope` gate.
--
-- ISOLATED from ops.sam_extraction_runs (extraction) and ops.sam_attachment_download_runs
-- (byte transport) by design. Idempotent DDL. CANONICAL COPY — the resolver mirrors this verbatim as
-- OPS_DDL and applies it (CREATE ... IF NOT EXISTS) before each insert. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_attachment_gtm_scope_runs (
    id                      bigserial PRIMARY KEY,
    run_id                  text        NOT NULL,   -- 'gtm-scope-<UTC ISO>' batch identifier
    universe_files          int,                    -- distinct downloaded resources verdicted
    in_scope                int,                    -- gtm_scope='in_scope'
    out_of_scope            int,                    -- gtm_scope='out_of_scope'
    r_out_of_scope_naics    int,                    -- reason: award NAICS not in target set
    r_below_band           int,                     -- reason: award < $500k
    r_above_band           int,                     -- reason: award > $15M
    r_failed_frequency_cap int,                     -- reason: entity > 30 in-band awards
    r_no_award_link        int,                     -- reason: resource maps to no prime award in the manifest
    cohort_in_band_awards  int,                     -- distinct awards passing Rule A + Rule B (pre frequency cap)
    capped_entities        int,                     -- distinct recipient_uei with > 30 in-band awards
    band_lo                bigint,                  -- Rule A lower bound (cents-free dollars)
    band_hi                bigint,                  -- Rule A upper bound
    freq_cap               int,                     -- Rule C threshold
    target_naics           jsonb,                   -- Rule B 6-digit code set (audit of the gate config)
    status                 text,                    -- 'success' | 'error'
    error                  text,
    started_at             timestamptz,
    completed_at           timestamptz
);

CREATE INDEX IF NOT EXISTS sam_attachment_gtm_scope_90day_runs_run_id_idx
    ON ops.sam_attachment_gtm_scope_runs (run_id);
CREATE INDEX IF NOT EXISTS sam_attachment_gtm_scope_90day_runs_started_at_idx
    ON ops.sam_attachment_gtm_scope_runs (started_at DESC);
