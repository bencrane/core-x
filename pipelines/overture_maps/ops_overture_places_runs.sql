-- Terminal-state table for the Overture Maps "Places" spatial bulk ingest worker.
-- Written by pipelines/overture_maps/places.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / SBA / SoS / UCC / PDL feeds (ARCHITECTURE.md
-- §5). Idempotent DDL. Also created at runtime by the worker's init_schema
-- (modal run ...::initdb); this file is the reviewable source.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.overture_places_runs (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed            text        NOT NULL,    -- 'overture_places'
    dataset_uri     text        NOT NULL,    -- s3://data-sink/active/overture_places/
    release_tag     text,                     -- resolved Overture release (YYYY-MM-DD.N)
    snapshot_date   date,                     -- ingest UTC date (monthly snapshot)
    rows_processed  bigint,                   -- committed Lance row count (US subset)
    distinct_ids    bigint,                   -- exact COUNT(DISTINCT id); NULL on stream path
    published_files bigint,                   -- files uploaded to R2 (data + indices)
    published_bytes bigint,                   -- total bytes published to R2
    write_path      text,                     -- 'materialize' | 'stream'
    status          text        NOT NULL,     -- 'success' | 'error'
    error           text,
    started_at      timestamptz NOT NULL,
    completed_at    timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS overture_places_runs_status_idx        ON ops.overture_places_runs (status);
CREATE INDEX IF NOT EXISTS overture_places_runs_snapshot_date_idx ON ops.overture_places_runs (snapshot_date DESC);
CREATE INDEX IF NOT EXISTS overture_places_runs_completed_at_idx  ON ops.overture_places_runs (completed_at DESC);
