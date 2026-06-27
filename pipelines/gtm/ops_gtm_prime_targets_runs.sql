-- ops.gtm_prime_targets_runs — per-run terminal-state ledger for the GTM prime-target
-- materializer (pipelines/gtm/gtm_prime_targets.py). One row per build invocation.
-- Lives in the hq-x control-plane DB (HQX_DB_URL_POOLED), alongside every other
-- ops.*_runs ledger.
--
-- The materializer applies this DDL defensively (IF NOT EXISTS) before each terminal
-- write, so it re-applies cleanly. Forward-only. IF NOT EXISTS throughout.
--
-- The dataset itself (s3://data-sink/active/gtm_prime_targets/) is the Lance SoR; this
-- table records the run-level counts + the ICP knobs in effect, so a cohort-size shift
-- across refreshes is attributable to either upstream data movement or a knob change.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gtm_prime_targets_runs (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                 text NOT NULL,                       -- 'gtm_prime_targets'
    as_of_date           date,                                -- govcon_prime_trajectories snapshot pinned
    band_lo              bigint,                              -- ICP: t24m obligated lower bound
    band_hi              bigint,                              -- ICP: t24m obligated upper bound
    min_t12m_awards      integer,                             -- ICP: recurring-cadence floor
    min_avg_award        bigint,                              -- ICP: avg $/award (commodity filter)
    cohort_rows          bigint NOT NULL DEFAULT 0,           -- total primes persisted
    known_rows           bigint NOT NULL DEFAULT 0,           -- known subcontractors (expand_roster)
    cold_rows            bigint NOT NULL DEFAULT 0,           -- cold (form_roster)
    total_obligated_t12m double precision,                    -- sum t12m obligations across cohort
    dataset_uri          text,                                -- R2 Lance SoR URI written
    status               text NOT NULL,                       -- 'success' | 'error'
    error                text,
    started_at           timestamptz,
    completed_at         timestamptz,
    recorded_at          timestamptz NOT NULL DEFAULT now()
);
