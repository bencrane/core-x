-- Audit ledger for the GovCon Prime Trajectories materialization worker.
-- Written by pipelines/usaspending/govcon_prime_trajectories.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- One row per build. The dataset is a full idempotent OVERWRITE (mode='overwrite'),
-- so the ledger is the rebuild history, not an append log. dup_collapsed records how
-- many deep↔live overlapping transactions were removed by the txn_key dedup;
-- null_txn_key_rows records rows kept un-deduped (no transaction key). Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.govcon_prime_trajectories_runs (
    id                      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                    text        NOT NULL,
    dataset_uri             text        NOT NULL,
    as_of_date              date,                   -- window anchor (build date)
    rows_written            bigint,                 -- = distinct recipient_uei
    bonded_uei              bigint,                 -- is_bonded_vertical = true
    deep_rows               bigint,                 -- transaction_search_fpds rows scanned
    live_rows               bigint,                 -- contract_prime_txn rows scanned
    union_rows              bigint,                 -- deep + live before dedup
    deduped_rows            bigint,                 -- after txn_key collapse
    dup_collapsed           bigint,                 -- union_rows - deduped_rows (overlap removed)
    null_txn_key_rows       bigint,                 -- rows kept un-deduped (no transaction key)
    new_awards_t24m_total   bigint,                 -- corpus sum (sanity)
    obligated_t24m_total    numeric,                -- corpus sum (sanity)
    status                  text        NOT NULL,   -- 'success' | 'error'
    error                   text,
    started_at              timestamptz,
    completed_at            timestamptz,
    recorded_at             timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS govcon_prime_trajectories_runs_feed_idx        ON ops.govcon_prime_trajectories_runs (feed);
CREATE INDEX IF NOT EXISTS govcon_prime_trajectories_runs_status_idx      ON ops.govcon_prime_trajectories_runs (status);
CREATE INDEX IF NOT EXISTS govcon_prime_trajectories_runs_recorded_at_idx ON ops.govcon_prime_trajectories_runs (recorded_at DESC);
