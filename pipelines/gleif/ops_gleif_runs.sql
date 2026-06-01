-- Terminal-state table for the GLEIF Golden Copy bulk ingest worker
-- (Level 1 LEI records / Level 2 relationship records).
-- Written by pipelines/gleif/ingest.py:_record_run via psycopg (HQX_DB_URL_POOLED)
-- on every terminal state, success or failure — mirrors the ops.* contract used by the
-- SAM / SBA / USPTO / CSLB feeds (ARCHITECTURE.md §5). Idempotent DDL.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.gleif_runs (
    id                     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    level                  text        NOT NULL,   -- 'l1' (LEI records) | 'l2' (relationships)
    feed                   text        NOT NULL,   -- 'gleif_l1_entities' | 'gleif_l2_relationships'
    run_mode               text        NOT NULL,   -- 'full_overwrite' (daily full golden-copy snapshot)
    write_mode             text,                   -- 'overwrite'
    dataset_uri            text,                   -- s3://data-sink/active/gleif_l*/
    publish_date           text,                   -- GLEIF publish_date of the consumed golden copy
    source_file            text,                   -- the .xml.zip basename downloaded
    record_count_published bigint,                 -- record_count the discovery API advertised
    rows_processed         bigint,                 -- rows actually parsed + written to Lance
    status                 text        NOT NULL,   -- 'success' | 'error'
    error                  text,
    started_at             timestamptz,
    completed_at           timestamptz,
    recorded_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS gleif_runs_level_idx        ON ops.gleif_runs (level);
CREATE INDEX IF NOT EXISTS gleif_runs_feed_idx         ON ops.gleif_runs (feed);
CREATE INDEX IF NOT EXISTS gleif_runs_status_idx       ON ops.gleif_runs (status);
CREATE INDEX IF NOT EXISTS gleif_runs_publish_date_idx ON ops.gleif_runs (publish_date DESC);
CREATE INDEX IF NOT EXISTS gleif_runs_recorded_at_idx  ON ops.gleif_runs (recorded_at DESC);
