-- ops.entity_hierarchy_runs — operational ledger for the federal entity-hierarchy
-- recompute (pipelines/resolution/entity_hierarchy.py). One row per build. Run-state
-- only; NOT the system of record (the hierarchy itself is Lance on R2). Idempotent DDL.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.entity_hierarchy_runs (
    id                bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed              text        NOT NULL,
    dataset_uri       text        NOT NULL,
    rows_written      bigint,             -- total child rows (children with a parent)
    immediate_edges   bigint,             -- children with a known immediate parent (rl ∪ govcon)
    sub_only_children bigint,             -- children with only an FSRS ultimate (subaward-only)
    cyclic_uei        bigint,             -- children whose immediate chain is a cycle (flagged)
    max_depth         integer,            -- deepest immediate→ultimate chain observed
    ultimate_parents  bigint,             -- distinct ultimate_parent_uei
    snapshot_date     date,
    status            text        NOT NULL,
    error             text,
    started_at        timestamptz,
    completed_at      timestamptz,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS entity_hierarchy_runs_feed_idx
    ON ops.entity_hierarchy_runs (feed);
CREATE INDEX IF NOT EXISTS entity_hierarchy_runs_status_idx
    ON ops.entity_hierarchy_runs (status);
CREATE INDEX IF NOT EXISTS entity_hierarchy_runs_recorded_at_idx
    ON ops.entity_hierarchy_runs (recorded_at DESC);
