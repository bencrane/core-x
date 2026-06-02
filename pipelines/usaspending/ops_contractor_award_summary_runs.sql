-- Terminal-state table for the USAspending Contractor Award Summary worker.
-- Written by pipelines/usaspending/contractor_award_summary.py:_record_run via
-- psycopg (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors
-- the ops.* contract used by the SAM / crosswalk / usaspending feeds
-- (ARCHITECTURE.md §5). One row per rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it via `modal run pipelines/usaspending/contractor_award_summary.py::init_ops`.
-- Keep the two in sync.
--
-- subaward_join_match_pct records the fraction of subaward rows whose
-- unique_award_key resolved to a current award_search.generated_unique_award_id
-- (the basis for subaward active/closed). Measured ~69% live; the ~31% miss are
-- overwhelmingly pre-2020 primes aged out of award_search → correctly closed.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.contractor_award_summary_runs (
    id                          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                        text        NOT NULL,   -- 'contractor_award_summary'
    dataset_uri                 text        NOT NULL,   -- s3://data-sink/active/contractor_award_summary/
    rows_written                bigint,                 -- summary rows (1 per recipient_uei)
    distinct_prime_uei          bigint,                 -- prime recipients (award_search.recipient_uei)
    distinct_subaward_uei       bigint,                 -- subawardees (subaward_search.sub_awardee_or_recipient_uei)
    distinct_combined_uei       bigint,                 -- union of the two; == rows_written
    lifetime_prime_obligated    numeric,                -- Σ award_search.total_obligation
    lifetime_subaward_obligated numeric,                -- Σ subaward_search.subaward_amount (sub received)
    total_combined_obligated    numeric,                -- prime + sub
    subaward_join_match_pct     numeric,                -- % of subawards whose prime PoP resolved
    status                      text        NOT NULL,   -- 'success' | 'error'
    error                       text,
    started_at                  timestamptz,
    completed_at                timestamptz,
    recorded_at                 timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS contractor_award_summary_runs_feed_idx
    ON ops.contractor_award_summary_runs (feed);
CREATE INDEX IF NOT EXISTS contractor_award_summary_runs_status_idx
    ON ops.contractor_award_summary_runs (status);
CREATE INDEX IF NOT EXISTS contractor_award_summary_runs_recorded_at_idx
    ON ops.contractor_award_summary_runs (recorded_at DESC);
