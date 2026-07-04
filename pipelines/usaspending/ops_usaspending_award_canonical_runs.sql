-- Ops ledger for the USAspending AWARD CANONICAL (award-summary spine) build (typed v2.1 SoR, GIANT).
-- Reconciles BULK (usaspending/award_search, contract scope generated_unique_award_id LIKE 'CONT%',
-- ~30,683,126 rows) + FRESH (usaspending_api_fresh/contract_prime_award) via a single-column-PK
-- per-key argmax on generated_unique_award_id: each source collapsed latest-per-gua, one flat 2-way
-- window (source_rank FRESH<BULK) → tie=FRESH; single-source enrich fill. A SEMI-JOIN-SCOPED
-- parent_award leg (usaspending/parent_award, filtered to the ~32,341 in-spine keys) supplies 10
-- net-new IDV rollup aggregates as a 1:1 dimensional enrich (NOT a union member). include_fresh is the
-- reconcile-later switch (FALSE=BULK-only first landing; TRUE=BULK⊕FRESH). NO monthly/delta/tombstone
-- (award_search has no correction_delete_ind). Overwrite read-model rebuild. One row per terminal
-- state, written by the worker's _record_run via psycopg (HQX_DB_URL_POOLED). Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.usaspending_award_canonical_runs (
    id                        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                      text        NOT NULL,   -- 'usaspending_award_canonical'
    include_fresh             boolean,                -- reconcile-later switch: FALSE=BULK-only
    rows_in_bulk              bigint,                 -- award_search rows scanned (CONT% contract scope)
    rows_in_fresh             bigint,                 -- contract_prime_award rows (0 when include_fresh=FALSE)
    rows_in_parent_award      bigint,                 -- parent_award rows AFTER semi-join filter (~32,341)
    parent_award_matched      bigint,                 -- canonical_out rows that received a parent-rollup enrich
    rows_out                  bigint,                 -- canonical rows written (post per-gua collapse)
    rows_out_floor            bigint,                 -- 0.90 * live bulk_scope (full-universe runs only)
    rows_out_floor_ok         boolean,                -- rows_out >= rows_out_floor
    dedup_collapsed           bigint,                 -- BULK leg ~0 (already 1:1); driven by FRESH re-pull dups
    fresh_only_tail           bigint,                 -- awards present ONLY in FRESH (freshness frontier)
    bulk_only_body            bigint,                 -- awards resolved canonical_source='bulk' (deep back-catalog)
    fresh_corrections_applied bigint,                 -- awards FRESH WON (landed download corrections)
    null_key_dropped          bigint,                 -- source rows dropped for NULL gua
    max_last_modified_date    timestamp,              -- max(last_modified_date) clamped <= now() (argmax frontier)
    max_action_date           date,                   -- max(action_date) clamped <= CURRENT_DATE (sentinel-safe)
    columns                   integer,                -- typed column count written (393)
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
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS include_fresh boolean;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS rows_in_parent_award bigint;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS parent_award_matched bigint;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS rows_out_floor bigint;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS rows_out_floor_ok boolean;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS max_last_modified_date timestamp;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS bulk_only_body bigint;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS fresh_corrections_applied bigint;
ALTER TABLE ops.usaspending_award_canonical_runs
    ADD COLUMN IF NOT EXISTS null_key_dropped bigint;
CREATE INDEX IF NOT EXISTS usaspending_award_canonical_runs_status_idx
    ON ops.usaspending_award_canonical_runs (status);
CREATE INDEX IF NOT EXISTS usaspending_award_canonical_runs_recorded_at_idx
    ON ops.usaspending_award_canonical_runs (recorded_at DESC);
CREATE INDEX IF NOT EXISTS usaspending_award_canonical_runs_max_lastmod_idx
    ON ops.usaspending_award_canonical_runs (max_last_modified_date DESC);
