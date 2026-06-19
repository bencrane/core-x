-- ops.sub_diversification_serving_runs — terminal-state ledger for the govcon_sub_diversification
-- serving worker (pipelines/serving/materialize_sub_diversification.py). One row per run.
-- Mirrored verbatim as OPS_DDL in that module (self-bootstrapping; idempotent).
-- Generalizes ops.captive_diversification_serving_runs: the captive segment is now a query-time
-- predicate (n_incumbent_primes = 1), not a baked substrate, so the ledger tracks the full sub
-- universe plus the single/multi-prime split.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sub_diversification_serving_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                   text,
    feed                     text        NOT NULL,
    window_days              integer,
    subs_scored              bigint,
    subs_with_vectors        bigint,
    rows_written             bigint,
    naics2_aligned_rows      bigint,
    naics4_aligned_rows      bigint,
    subs_with_naics2_match   bigint,
    single_prime_subs        bigint,
    multi_prime_subs         bigint,
    distinct_new_primes      bigint,
    matchable_awards         bigint,
    avg_match_score          numeric,
    write_mode               text,
    indices_built            text,
    status                   text        NOT NULL,
    error_message            text,
    metrics                  jsonb,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sub_diversification_serving_runs_status_idx
    ON ops.sub_diversification_serving_runs (status);
CREATE INDEX IF NOT EXISTS sub_diversification_serving_runs_recorded_at_idx
    ON ops.sub_diversification_serving_runs (recorded_at DESC);
