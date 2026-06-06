-- Terminal-state table for the SoS → SAM (name → UEI) crosswalk worker.
-- Written by pipelines/resolution/crosswalk_sos_sam.py:_record_run via psycopg
-- (HQX_DB_URL_POOLED) on every terminal state, success or failure — mirrors the ops.*
-- contract used by the SAM / crosswalk / usaspending feeds (ARCHITECTURE.md §5). One row
-- per crosswalk rebuild. Idempotent DDL.
--
-- CANONICAL COPY. The worker mirrors this verbatim as the OPS_DDL constant and applies it
-- via `modal run pipelines/resolution/crosswalk_sos_sam.py::init_ops`. Keep in sync.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.crosswalk_sos_sam_runs (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed                text        NOT NULL,   -- 'crosswalk_sos_sam'
    dataset_uri         text        NOT NULL,
    rows_written        bigint,                 -- all candidate pairs (v9 ~942k)
    canonical_rows      bigint,                 -- is_canonical rows == distinct SoS entities with a tier-1..4 match (v9 ~527k)
    distinct_uei        bigint,                 -- federal entities reached, all-tier union (v9 ~443k)
    distinct_sos_entity bigint,                 -- SoS entities matched, all-tier (v9 ~743k)
    tier1_rows          bigint,                 -- exact + state + zip (the high-precision tier-1; v9 ~207k)
    tier5_rows          bigint,                 -- base + no-geo (the unsafe recall tier, never canonical; v9 ~313k)
    status              text        NOT NULL,   -- 'success' | 'error'
    error               text,
    started_at          timestamptz,
    completed_at        timestamptz,
    recorded_at         timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_feed_idx        ON ops.crosswalk_sos_sam_runs (feed);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_status_idx      ON ops.crosswalk_sos_sam_runs (status);
CREATE INDEX IF NOT EXISTS crosswalk_sos_sam_runs_recorded_at_idx ON ops.crosswalk_sos_sam_runs (recorded_at DESC);
