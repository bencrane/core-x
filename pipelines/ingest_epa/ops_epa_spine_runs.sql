-- Canonical DDL mirror for the EPA Unified Facility Spine ops ledger.
-- Applied (idempotently) by materialize_epa_spine.py::apply_state_schema via
-- _ensure_ops_ledger() once per orchestrator run, before any fan-out. IF-NOT-EXISTS
-- DDL is NOT concurrency-safe across many workers (catalog-lock deadlocks), so the
-- orchestrator creates it up front and the workers' _record_run only self-bootstrap
-- on direct single-container invocations.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.epa_spine_runs (
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id        text  NOT NULL,
    phase         text  NOT NULL,           -- preflight|crosswalk|spine|rollup|capstone|verify
    artifact      text  NOT NULL,
    dataset_uri   text,
    grain         text,
    rows_written  bigint,
    reach_pct     double precision,         -- key-resolution reach for crosswalks/rollups
    null_key_pct  double precision,
    indices_built text,
    gates         jsonb,
    status        text  NOT NULL,
    error         text,
    started_at    timestamptz,
    completed_at  timestamptz,
    recorded_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS epa_spine_runs_run_idx      ON ops.epa_spine_runs (run_id);
CREATE INDEX IF NOT EXISTS epa_spine_runs_artifact_idx ON ops.epa_spine_runs (artifact);
CREATE INDEX IF NOT EXISTS epa_spine_runs_phase_idx    ON ops.epa_spine_runs (phase);
CREATE INDEX IF NOT EXISTS epa_spine_runs_status_idx   ON ops.epa_spine_runs (status);
CREATE INDEX IF NOT EXISTS epa_spine_runs_recorded_idx ON ops.epa_spine_runs (recorded_at DESC);
