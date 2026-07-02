-- Ops ledger for the USAspending SUBAWARD CANONICAL table build (typed v2 SoR).
-- Reconciles BULK (usaspending/subaward_search, contract-only prime_award_group='procurement') +
-- FRESH (usaspending_api_fresh/contract_subaward). TWO-SOURCE per-composite argmax on
-- (prime_award_unique_key, subaward_number): each source collapsed latest-per-composite, one flat
-- 2-way window (source_rank FRESH<BULK) → tie=FRESH; single-source enrich fill. No monthly/delta/
-- tombstone. Overwrite read-model rebuild. One row per terminal state, written by the worker's
-- _record_run via psycopg (HQX_DB_URL_POOLED). Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.usaspending_subaward_canonical_runs (
    id                        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                      text        NOT NULL,   -- 'usaspending_subaward_canonical'
    rows_in_bulk_contract     bigint,                 -- BULK rows scanned (contract-only scope)
    rows_in_fresh             bigint,                 -- FRESH rows scanned (procurement-only feed)
    rows_out                  bigint,                 -- canonical rows written (post per-composite collapse)
    dedup_collapsed           bigint,                 -- rows dropped by the latest-per-composite collapse
    fresh_only_tail           bigint,                 -- composites present ONLY in FRESH (freshness frontier)
    bulk_only_body            bigint,                 -- composites present ONLY in BULK (deep back-catalog)
    fresh_corrections_applied bigint,                 -- composites FRESH WON that BULK also holds (landed FSRS corrections)
    null_key_dropped          bigint,                 -- source rows dropped for NULL composite (missing prime link / sub number)
    max_subaward_action_date  date,                   -- max(subaward_action_date) clamped to <= CURRENT_DATE (FSRS sentinel-safe)
    columns                   integer,                -- typed column count written
    write_mode                text,                   -- 'overwrite'
    indices_built             text,                   -- comma list of BTREE/BITMAP columns
    status                    text        NOT NULL,   -- 'success' | 'error'
    error_message             text,                   -- NEVER null when status<>'success'
    started_at                timestamptz,
    completed_at              timestamptz,
    recorded_at               timestamptz NOT NULL DEFAULT now()
);
-- Idempotent forward-fill for schemas initialized before a column existed (CREATE TABLE IF NOT
-- EXISTS never ALTERs an existing table).
ALTER TABLE ops.usaspending_subaward_canonical_runs
    ADD COLUMN IF NOT EXISTS bulk_only_body bigint;
ALTER TABLE ops.usaspending_subaward_canonical_runs
    ADD COLUMN IF NOT EXISTS fresh_corrections_applied bigint;
ALTER TABLE ops.usaspending_subaward_canonical_runs
    ADD COLUMN IF NOT EXISTS null_key_dropped bigint;
CREATE INDEX IF NOT EXISTS usaspending_subaward_canonical_runs_status_idx
    ON ops.usaspending_subaward_canonical_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_subaward_canonical_runs_recorded_at_idx
    ON ops.usaspending_subaward_canonical_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS usaspending_subaward_canonical_runs_max_action_date_idx
    ON ops.usaspending_subaward_canonical_runs (max_subaward_action_date DESC);
