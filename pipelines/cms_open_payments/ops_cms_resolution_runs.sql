-- Operational ledger for the CMS Open Payments resolution layer
-- (pipelines/cms_open_payments/materialize_resolution.py). One row per (artifact, run).
-- Mirrored verbatim by the module's OPS_DDL; applied by ::init_state.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.cms_resolution_runs (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    feed           text        NOT NULL,
    artifact       text        NOT NULL,
    dataset_uri    text,
    rows_written   bigint,
    indices        text,
    status         text        NOT NULL,
    error          text,
    started_at     timestamptz,
    completed_at   timestamptz,
    recorded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cms_resolution_runs_feed_idx     ON ops.cms_resolution_runs (feed);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_artifact_idx ON ops.cms_resolution_runs (artifact);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_status_idx   ON ops.cms_resolution_runs (status);
CREATE INDEX IF NOT EXISTS cms_resolution_runs_recorded_idx ON ops.cms_resolution_runs (recorded_at DESC);
