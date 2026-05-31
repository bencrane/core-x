-- Terminal-state table for the CA Secretary of State business-records ingest.
-- Written by pipelines/ca_sos/entities_bulk.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SBA/SAM feeds (ARCHITECTURE.md §5). Idempotent DDL.
--
-- This DDL is mirrored verbatim by the OPS_DDL constant in entities_bulk.py; the
-- worker's `init_state` entrypoint applies it. Keep the two in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.ca_sos_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    phase          text        NOT NULL,   -- 'explode' | 'ingest'
    member         text,                   -- 'entities' | 'agents' | 'principals' (NULL for explode)
    dataset_uri    text,                   -- s3://data-sink/active/ca_sos_<member>/
    as_of          date,                   -- explicit export date (snapshot_date)
    source_zip     text,                   -- landing key of the source ZIP
    landing_key    text,                   -- per-member .csv.zst landing key (ingest phase)
    rows_processed bigint,
    rejected_rows  bigint,                 -- ragged rows quarantined by store_rejects
    status         text        NOT NULL,   -- 'success' | 'error'
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ca_sos_runs_member_idx      ON ops.ca_sos_runs (member);
CREATE INDEX IF NOT EXISTS ca_sos_runs_phase_idx       ON ops.ca_sos_runs (phase);
CREATE INDEX IF NOT EXISTS ca_sos_runs_status_idx      ON ops.ca_sos_runs (status);
CREATE INDEX IF NOT EXISTS ca_sos_runs_recorded_at_idx ON ops.ca_sos_runs (recorded_at DESC);
