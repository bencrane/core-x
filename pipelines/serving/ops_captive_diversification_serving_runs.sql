-- Audit ledger for the captive-sub diversification serving worker.
-- Written by pipelines/serving/materialize_captive_diversification.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- DERIVED / rebuildable: captive_sub_diversification_90day is a read model (overwrite each run).
-- It scores every single-prime "captive" subawardee (band-bounded) by its BGE capability vector
-- against the prime-solicitation scope corpus (ANN) to surface fresh awards won by OTHER primes
-- the sub is a domain-aligned capability match for — the "diversify off your one prime" target list.
-- The window (award action_date) and the captive band are build parameters, not data properties.
-- Idempotent DDL; mirrored verbatim as the OPS_DDL constant in the worker.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.captive_diversification_serving_runs (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 text,
    feed                   text        NOT NULL,
    window_days            integer,                -- rolling award action_date window
    captive_subs_scored    bigint,                 -- captive segment size (single-prime, recurring, banded)
    subs_with_vectors      bigint,                 -- captive subs that carry a BGE capability vector
    rows_written           bigint,                 -- (captive_sub, candidate_new_prime) rows materialized
    naics2_aligned_rows    bigint,                 -- usable tier: award NAICS sector == sub trade sector
    naics4_aligned_rows    bigint,                 -- high-precision tier: NAICS-4 match
    subs_with_naics2_match bigint,                 -- distinct captive subs with >=1 sector-aligned match
    distinct_new_primes    bigint,                 -- distinct candidate (different) primes surfaced
    matchable_awards       bigint,                 -- distinct awards hit (bounded by harvested scope text)
    avg_match_score        numeric,                -- mean cosine similarity (1 - distance)
    write_mode             text,                   -- 'overwrite'
    indices_built          text,                   -- comma list of BTREE+BITMAP columns
    status                 text        NOT NULL,   -- 'success' | 'error'
    error_message          text,
    metrics                jsonb,                  -- full stats payload (forward-compatible)
    started_at             timestamptz,
    completed_at           timestamptz,
    recorded_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS captive_diversification_serving_runs_status_idx
    ON ops.captive_diversification_serving_runs (status);
CREATE INDEX IF NOT EXISTS captive_diversification_serving_runs_recorded_at_idx
    ON ops.captive_diversification_serving_runs (recorded_at DESC);
