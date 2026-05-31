-- Terminal-state table for the USPTO Trademark bulk ingest worker
-- (applications / assignments / TTAB).
-- Written by pipelines/uspto_tm/ingest.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / SBA / CO-SoS feeds (ARCHITECTURE.md §5).
-- Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.uspto_tm_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset         text        NOT NULL,   -- 'applications' | 'assignments' | 'ttab'
    feed            text        NOT NULL,   -- 'uspto_tm_applications' | 'uspto_tm_assignments' | 'uspto_tm_ttab'
    run_mode        text        NOT NULL,   -- 'backfile' | 'delta'
    write_mode      text,                   -- 'overwrite+append' | 'merge_insert' | 'create'
    dataset_uri     text,                   -- s3://data-sink/active/uspto_tm_*/
    as_of           text,                   -- file date stamp (YYMMDD) or backfile cut label
    source_files    jsonb,                  -- list of zip basenames processed this run
    parts_processed integer,                -- backfile parts streamed (1 for a delta)
    rows_processed  bigint,                 -- records transcoded + projected
    rows_upserted   bigint,                 -- rows merged/written into Lance
    status          text        NOT NULL,   -- 'success' | 'error'
    error           text,
    started_at      timestamptz,
    completed_at    timestamptz,
    recorded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS uspto_tm_runs_dataset_idx     ON ops.uspto_tm_runs (dataset);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_feed_idx        ON ops.uspto_tm_runs (feed);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_status_idx      ON ops.uspto_tm_runs (status);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_as_of_idx       ON ops.uspto_tm_runs (as_of DESC);
CREATE INDEX IF NOT EXISTS uspto_tm_runs_recorded_at_idx ON ops.uspto_tm_runs (recorded_at DESC);
