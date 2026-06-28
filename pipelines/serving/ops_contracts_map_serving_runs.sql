-- Audit ledger for the contract-grain map serving worker.
-- Written by pipelines/serving/materialize_contracts_map.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure.
--
-- DERIVED / rebuildable: usaspending_contracts_map_serving is a read model (overwrite each
-- run): ONE ROW PER CONTRACT (contract_award_unique_key) rolled from the prime transaction
-- ledger. The prime feed is deduped on contract_transaction_unique_key, then summed to award
-- grain: contract_obligated_usd = NET sum(federal_action_obligation) (de-obligations INCLUDED
-- so a deobligated contract reads its true current value), contract_ceiling_usd =
-- arg_max(base_and_all_options_value by action_date), action_count = # distinct transactions.
-- HAVING net > 0 drops fully-deobligated / cancelled contracts. PRIME-ONLY (subawards have no
-- PIID). Joined to geocode_xwalk on addr_hash. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.contracts_map_serving_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    window_days    integer,                -- rolling action_date window
    rows_written   bigint,                 -- contracts (post dedupe + award-grain roll, net > 0)
    prime_rows     bigint,                 -- prime contracts (== rows_written; prime-only build)
    action_total   bigint,                 -- distinct transactions rolled across all contracts
    with_coords    bigint,                 -- contracts that joined a geocode_xwalk dot
    coord_rate     numeric,                -- with_coords / rows_written
    write_mode     text,                   -- 'overwrite'
    indices_built  text,                   -- comma list of BTREE+BITMAP columns
    status         text        NOT NULL,   -- 'success' | 'error'
    error_message  text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS contracts_map_serving_runs_status_idx
    ON ops.contracts_map_serving_runs (status);
CREATE INDEX IF NOT EXISTS contracts_map_serving_runs_recorded_at_idx
    ON ops.contracts_map_serving_runs (recorded_at DESC);
