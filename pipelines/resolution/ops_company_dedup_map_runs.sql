-- ops.company_dedup_map_runs — operational ledger for the company dedup crosswalk
-- (pipelines/resolution/company_dedup_map.py). One row per build. Run-state only;
-- NOT the system of record (the map itself is Lance on R2). Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.company_dedup_map_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    dataset_uri       text        NOT NULL,
    rows_written      bigint,             -- 1 per company_id (== companies_canonical rows)
    canonical_groups  bigint,             -- distinct canonical_company_id
    merges            bigint,             -- rows_written - canonical_groups
    merged_by_uei     bigint,             -- rows collapsed by the exact-UEI tier
    merged_by_name    bigint,             -- rows collapsed by the UEI-null name-gated tier
    max_group_size    integer,
    snapshot_date     date,
    status            text        NOT NULL,
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS company_dedup_map_runs_feed_idx
    ON ops.company_dedup_map_runs (feed);
CREATE INDEX IF NOT EXISTS company_dedup_map_runs_status_idx
    ON ops.company_dedup_map_runs (status);
CREATE INDEX IF NOT EXISTS company_dedup_map_runs_recorded_at_idx
    ON ops.company_dedup_map_runs (recorded_at DESC);
