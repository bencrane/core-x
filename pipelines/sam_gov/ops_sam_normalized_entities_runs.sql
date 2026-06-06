-- Terminal-state table for the SAM.gov normalized-name → UEI resolution sidecar worker.
-- Written by pipelines/sam_gov/sam_normalized_entities.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the ops.*
-- contract used by the SAM / crosswalk / usaspending feeds (ARCHITECTURE.md §5). One row
-- per sidecar rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies it
-- via `modal run pipelines/sam_gov/sam_normalized_entities.py::init_ops`. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.sam_normalized_entities_runs (
    id                       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                     text        NOT NULL,   -- 'sam_normalized_entities'
    dataset_uri              text        NOT NULL,   -- s3://data-sink/active/sam_normalized_entities/
    source_uri               text,                   -- s3://data-sink/active/sam_master_entities/
    sam_extract_label        text,                   -- provenance carried from the mirror (max label)
    rows_written             bigint,                 -- 1 per uei (passthrough)
    distinct_uei             bigint,                 -- == rows_written (uniqueness gate)
    distinct_normalized_name bigint,                 -- exact blocking-key cardinality
    distinct_legal_name_base bigint,                 -- suffix-peeled blocking-key cardinality (§B9 floor)
    status                   text        NOT NULL,   -- 'success' | 'error'
    error                    text,
    started_at               timestamptz,
    completed_at             timestamptz,
    recorded_at              timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_feed_idx
    ON ops.sam_normalized_entities_runs (feed);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_status_idx
    ON ops.sam_normalized_entities_runs (status);
CREATE INDEX IF NOT EXISTS sam_normalized_entities_runs_recorded_at_idx
    ON ops.sam_normalized_entities_runs (recorded_at DESC);
