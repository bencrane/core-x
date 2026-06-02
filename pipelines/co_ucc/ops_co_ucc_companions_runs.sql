-- Terminal-state table for the Colorado UCC companion-datasets migration worker.
-- Written by pipelines/co_ucc/companions_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the ledger / SAM / SBA feeds (ARCHITECTURE.md §5). Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies
-- it via `modal run pipelines/co_ucc/companions_bulk.py::init_ops`. Keep the two in sync.
--
-- One run migrates the three Gen-2 companion streams
-- (s3://dex-raw-landing-zone/ucc/state=CO/stream={debtors,secured_parties,collateral}/)
-- into the Gen-3 active sink (s3://data-sink/active/ucc_co_{debtors,secured_parties,collateral}/).
-- ``datasets`` is the per-table row count map, e.g. {"debtors": n, "secured_parties": n, "collateral": n}.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.co_ucc_companions_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,   -- 'co_ucc_companions'
    source_bucket  text        NOT NULL,   -- Gen-2 source bucket ('dex-raw-landing-zone')
    snapshot_date  date,                   -- the migrated snapshot= partition
    datasets       jsonb       NOT NULL,   -- {"debtors": n, "secured_parties": n, "collateral": n}
    rows_total     bigint      NOT NULL DEFAULT 0,
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_feed_idx        ON ops.co_ucc_companions_runs (feed);
CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_status_idx      ON ops.co_ucc_companions_runs (status);
CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_snapshot_idx    ON ops.co_ucc_companions_runs (snapshot_date);
CREATE INDEX IF NOT EXISTS co_ucc_companions_runs_recorded_at_idx ON ops.co_ucc_companions_runs (recorded_at DESC);
