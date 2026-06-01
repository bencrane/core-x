-- Terminal-state table for the SAM.gov × USAspending canonical crosswalk worker.
-- Written by pipelines/resolution/crosswalk_sam_usaspending.py:_record_run via
-- psycopg (HQX_DB_URL_POOLED) on every terminal state, success or failure —
-- mirrors the ops.* contract used by the SAM / co_ucc / usaspending feeds
-- (ARCHITECTURE.md §5). One row per crosswalk rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and
-- applies it via `modal run pipelines/resolution/crosswalk_sam_usaspending.py::init_ops`.
-- Keep the two in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_sam_usaspending_runs (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed             text        NOT NULL,   -- 'crosswalk_sam_usaspending'
    dataset_uri      text        NOT NULL,   -- s3://data-sink/active/crosswalk_sam_usaspending/
    sam_label        text,                   -- latest v2 entity_registrations extract_label (provenance)
    rows_written     bigint,                 -- crosswalk rows == distinct recipient_lookup UEI
    matched_by_uei   bigint,                 -- direct UEI matches into the SAM v2 registry
    matched_by_cage  bigint,                 -- CAGE-recovered (the legacy defense tail)
    matched_any      bigint,                 -- total SAM-resolved (uei + cage)
    status           text        NOT NULL,   -- 'success' | 'error'
    error            text,
    started_at       timestamptz,
    completed_at     timestamptz,
    recorded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_sam_usaspending_runs_feed_idx
    ON ops.crosswalk_sam_usaspending_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sam_usaspending_runs_status_idx
    ON ops.crosswalk_sam_usaspending_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sam_usaspending_runs_recorded_at_idx
    ON ops.crosswalk_sam_usaspending_runs (recorded_at DESC);
