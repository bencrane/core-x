-- Terminal-state table for the SAM.gov POC human-layer worker.
-- Written by pipelines/sam_gov/sam_pocs.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the
-- ops.* contract used by the SAM / crosswalk / usaspending feeds (ARCHITECTURE.md
-- §5). One row per POC rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it via `modal run pipelines/sam_gov/sam_pocs.py::init_ops`. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_pocs_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,   -- 'sam_pocs'
    dataset_uri      text        NOT NULL,   -- s3://data-sink/active/sam_pocs/
    sam_label        text,                   -- latest v2 entity_registrations extract_label (provenance)
    rows_written     bigint,                 -- total POC rows (1 per entity × populated slot)
    distinct_uei     bigint,                 -- v2 entities with ≥1 POC
    distinct_cage    bigint,                 -- legacy (uei-less) entities with ≥1 POC
    poc_rows_v2      bigint,                 -- POC rows sourced from the v2 registry
    poc_rows_legacy  bigint,                 -- POC rows sourced from the 120-wide legacy tail
    status           text        NOT NULL,   -- 'success' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sam_pocs_runs_feed_idx
    ON ops.sam_pocs_runs (feed);
CREATE INDEX IF NOT EXISTS sam_pocs_runs_status_idx
    ON ops.sam_pocs_runs (status);
CREATE INDEX IF NOT EXISTS sam_pocs_runs_recorded_at_idx
    ON ops.sam_pocs_runs (recorded_at DESC);
