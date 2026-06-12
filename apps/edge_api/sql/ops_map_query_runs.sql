-- QA ledger for the map natural-language query path (the cockpit's ⌘K "Query the Market").
-- Written by apps/edge_api/src/routers/map_ask_v1.py:_write_query_run via the edge_api
-- async pool (HQX_DB_URL_POOLED) on EVERY terminal state — success, translate failure,
-- execute failure. Fire-and-forget: a ledger error never blocks or fails the response.
--
-- One row per /ask round-trip: the raw sentence, the compiled filter object, the
-- `dropped` (unmapped) constraints the compiler could not express — the lagging QA
-- stamp that makes a silently-degraded translation auditable. Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.map_query_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts               timestamptz NOT NULL DEFAULT now(),
    dataset          text        NOT NULL,   -- 'company' | 'winners'
    raw_query        text        NOT NULL,   -- the operator's NL sentence, verbatim
    title            text,                   -- the compiler's human label
    compiled_filters jsonb,                  -- [{field, op, value}, ...]
    dropped          jsonb,                  -- unmapped constraint phrases ([] = all mapped)
    decoder_version  text,                   -- e.g. 'company.v3' — the allowlist in force
    result_count     integer,                -- meta.returned (plottable features served)
    total_count      integer,                -- meta.total (exact match count, pre-cap)
    capped           boolean,                -- result truncated at the row bound
    latency_ms       integer,                -- whole /ask round-trip (translate + execute)
    status           text        NOT NULL,   -- 'success' | 'translate_error' | 'execute_error'
    error_message    text
);

CREATE INDEX IF NOT EXISTS map_query_runs_ts_idx
    ON ops.map_query_runs (ts DESC);
CREATE INDEX IF NOT EXISTS map_query_runs_status_idx
    ON ops.map_query_runs (status);
